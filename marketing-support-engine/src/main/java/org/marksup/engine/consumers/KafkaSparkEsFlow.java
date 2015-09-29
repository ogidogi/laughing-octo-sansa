package org.marksup.engine.consumers;

import kafka.serializer.StringDecoder;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.marksup.engine.spark.sql.udf.ParseCoordinates;
import org.marksup.engine.spark.sql.udf.ParseUserAgentString;
import org.marksup.engine.spark.sql.udf.ParseUserTagsArray;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.callUDF;
import static org.marksup.engine.utils.MapperConstants.MappingSchemas.*;
import static org.marksup.engine.utils.MapperConstants.SchemaFields.*;

public class KafkaSparkEsFlow {
    private static final Logger log = Logger.getLogger(KafkaSparkEsFlow.class);
    private static final String dictDir = "/media/sf_Download/data/mors/new_dicts";

    public static void main(String[] args) throws ConfigurationException {
        KafkaSparkEsFlow workflow = new KafkaSparkEsFlow();
        log.setLevel(Level.DEBUG);

        CompositeConfiguration conf = new CompositeConfiguration();
        conf.addConfiguration(new PropertiesConfiguration("kafka.properties"));
        conf.addConfiguration(new PropertiesConfiguration("spark.properties"));
        conf.addConfiguration(new PropertiesConfiguration("es.properties"));

        try {
            workflow.run(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void run(CompositeConfiguration conf) {
        // Kafka props
        String kafkaBrokers = conf.getString("metadata.broker.list");
        String topics = conf.getString("consumer.topic");
        String fromOffset = conf.getString("auto.offset.reset");

        // Spark props
        String sparkMaster = conf.getString("spark.master");
        String sparkSerDe = conf.getString("spark.serializer");
        long sparkStreamDuration = conf.getLong("stream.duration");

        // Es props
        String esIndex = "rtb_test/log_item";
        String esIdxAutoCreate = conf.getString("es.index.auto.create");

        SparkConf sparkConf = new SparkConf().setAppName("Kafka Spark ES Flow with Java API").setMaster(sparkMaster)
                .set("es.index.auto.create", esIdxAutoCreate).set("spark.serializer", sparkSerDe);

        JavaSparkContext sp = new JavaSparkContext(sparkConf);
        JavaStreamingContext jssc = new JavaStreamingContext(sp, Durations.seconds(sparkStreamDuration));

        HashSet<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        HashMap<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", kafkaBrokers);
        kafkaParams.put("auto.offset.reset", fromOffset);

        // Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class,
                StringDecoder.class, kafkaParams, topicsSet);

        messages.foreachRDD(rdd -> {
            SQLContext sqlContext = SQLContext.getOrCreate(rdd.context());
            // AdExchange Dict
            DataFrame adExchangeDf = sqlContext.read().format("com.databricks.spark.csv").schema(AD_EXCH_SCHEMA.getSchema())
                    .option("header", "false").option("delimiter", "\t").load(Paths.get(dictDir, "ad.exchange.txt").toString());

            // Log Type Dict
            DataFrame logTypeDf = sqlContext.read().format("com.databricks.spark.csv").schema(LOG_TYPE_SCHEMA.getSchema())
                    .option("header", "false").option("delimiter", "\t").load(Paths.get(dictDir, "log.type.txt").toString());

            // City Dict
            DataFrame cityDf = sqlContext.read().format("com.databricks.spark.csv").schema(CITY_SCHEMA.getSchema())
                    .option("header", "false").option("delimiter", "\t").load(Paths.get(dictDir, "city.us.txt").toString());

            // US State Dict
            DataFrame stateDf = sqlContext.read().format("com.databricks.spark.csv").schema(STATE_SCHEMA.getSchema())
                    .option("header", "false").option("delimiter", "\t").load(Paths.get(dictDir, "states.us.txt").toString());

            JavaRDD<Row> rowRdd = rdd.map(x -> new GenericRow(x._2().split("\t")));

            // Workaround since there is no automatic type conversion between Row and DF
            StructType kafkaStrSchema = DataTypes.createStructType(Arrays.stream(BID_SCHEMA.getSchema().fields())
                    .map(field -> DataTypes.createStructField(field.name(), DataTypes.StringType, field.nullable()))
                    .collect(Collectors.toList()));
            DataFrame bidDf = sqlContext.createDataFrame(rowRdd, kafkaStrSchema);

            DataFrame parsedBidDf = bidDf
                    .join(adExchangeDf,
                            bidDf.col(AD_EXCH_ID.getStructField().name()).equalTo(adExchangeDf.col(AD_EXCH_ID.getStructField().name())),
                            "left")
                    .join(logTypeDf,
                            bidDf.col(LOG_TYPE_ID.getStructField().name()).equalTo(logTypeDf.col(LOG_TYPE_ID.getStructField().name())),
                            "left")
                    .join(cityDf, bidDf.col(CITY_ID.getStructField().name()).equalTo(cityDf.col(CITY_ID.getStructField().name())), "inner") // src data is messed a bit, so geo_point results in null ->
                                                                                                                                            // 'inner' join is workaround
                    .join(stateDf, cityDf.col(STATE_ID.getStructField().name()).equalTo(stateDf.col(STATE_ID.getStructField().name())),
                            "left")
                    .withColumn(USER_TAGS_ARRAY.getStructField().name(),
                            callUDF(new ParseUserTagsArray(), DataTypes.createArrayType(DataTypes.StringType),
                                    bidDf.col(USER_TAGS.getStructField().name())))
                    .withColumn(COORDINATES.getStructField().name(),
                            callUDF(new ParseCoordinates(), DataTypes.createArrayType(DataTypes.FloatType),
                                    cityDf.col(CITY_LATITUDE.getStructField().name()), cityDf.col(CITY_LONGITUDE.getStructField().name())))
                    // User Info
                    // -- browser
                    .withColumn(UA_BROWSER.getStructField().name(),
                            callUDF(new ParseUserAgentString(UA_BROWSER), DataTypes.StringType,
                                    bidDf.col(USER_AGENT.getStructField().name())))
                    .withColumn(UA_BROWSER_GROUP.getStructField().name(),
                            callUDF(new ParseUserAgentString(UA_BROWSER_GROUP), DataTypes.StringType,
                                    bidDf.col(USER_AGENT.getStructField().name())))
                    .withColumn(UA_BROWSER_MANUFACTURER.getStructField().name(),
                            callUDF(new ParseUserAgentString(UA_BROWSER_MANUFACTURER), DataTypes.StringType,
                                    bidDf.col(USER_AGENT.getStructField().name())))
                    .withColumn(UA_BROWSER_RENDERING_ENGINE.getStructField().name(),
                            callUDF(new ParseUserAgentString(UA_BROWSER_RENDERING_ENGINE), DataTypes.StringType,
                                    bidDf.col(USER_AGENT.getStructField().name())))
                    .withColumn(UA_BROWSERVERSION.getStructField().name(),
                            callUDF(new ParseUserAgentString(UA_BROWSERVERSION), DataTypes.StringType,
                                    bidDf.col(USER_AGENT.getStructField().name())))
                    .withColumn(UA_BROWSERVERSION_MINOR.getStructField().name(),
                            callUDF(new ParseUserAgentString(UA_BROWSERVERSION_MINOR), DataTypes.StringType,
                                    bidDf.col(USER_AGENT.getStructField().name())))
                    .withColumn(UA_BROWSERVERSION_MAJOR.getStructField().name(),
                            callUDF(new ParseUserAgentString(UA_BROWSERVERSION_MAJOR), DataTypes.StringType,
                                    bidDf.col(USER_AGENT.getStructField().name())))
                    // -- id
                    .withColumn(UA_ID.getStructField().name(),
                            callUDF(new ParseUserAgentString(UA_ID), DataTypes.StringType, bidDf.col(USER_AGENT.getStructField().name())))
                    // -- OS
                    .withColumn(UA_OS.getStructField().name(),
                            callUDF(new ParseUserAgentString(UA_OS), DataTypes.StringType, bidDf.col(USER_AGENT.getStructField().name())))
                    .withColumn(UA_OS_NAME.getStructField().name(),
                            callUDF(new ParseUserAgentString(UA_OS_NAME), DataTypes.StringType,
                                    bidDf.col(USER_AGENT.getStructField().name())))
                    .withColumn(UA_OS_DEVICE.getStructField().name(),
                            callUDF(new ParseUserAgentString(UA_OS_DEVICE), DataTypes.StringType,
                                    bidDf.col(USER_AGENT.getStructField().name())))
                    .withColumn(UA_OS_GROUP.getStructField().name(),
                            callUDF(new ParseUserAgentString(UA_OS_GROUP), DataTypes.StringType,
                                    bidDf.col(USER_AGENT.getStructField().name())))
                    .withColumn(UA_OS_MANUFACTURER.getStructField().name(), callUDF(new ParseUserAgentString(UA_OS_MANUFACTURER),
                            DataTypes.StringType, bidDf.col(USER_AGENT.getStructField().name())))

            .drop(bidDf.col(AD_EXCH_ID.getStructField().name())).drop(bidDf.col(LOG_TYPE_ID.getStructField().name()))
                    .drop(bidDf.col(CITY_ID.getStructField().name())).drop(bidDf.col(REGION.getStructField().name()));

            log.info("PARSED DATA");
            parsedBidDf.show();
//            JavaEsSpark.saveJsonToEs(parsedBidDf.toJSON().toJavaRDD(), esIndex);

            return null;
        });

        jssc.start();
        jssc.awaitTermination();

    }
}
