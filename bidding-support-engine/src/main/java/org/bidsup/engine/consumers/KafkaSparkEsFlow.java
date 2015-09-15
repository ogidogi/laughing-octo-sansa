package org.bidsup.engine.consumers;

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
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.bidsup.engine.spark.sql.udf.ParseCoordinates;
import org.bidsup.engine.spark.sql.udf.ParseUserAgentString;
import org.bidsup.engine.spark.sql.udf.ParseUserTagsArray;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import static org.apache.spark.sql.functions.callUDF;
import static org.bidsup.engine.utils.MapperConstants.MappingSchemas.*;
import static org.bidsup.engine.utils.MapperConstants.SchemaFields.*;

public class KafkaSparkEsFlow {
    private static final Logger log = Logger.getLogger(KafkaSparkCassandraFlow.class);

    public static void main(String[] args) throws ConfigurationException {
        KafkaSparkEsFlow workflow = new KafkaSparkEsFlow();
        log.setLevel(Level.DEBUG);

        CompositeConfiguration conf = new CompositeConfiguration();
        conf.addConfiguration(new PropertiesConfiguration("kafka.properties"));
        conf.addConfiguration(new PropertiesConfiguration("spark.properties"));

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
        long sparkStreamDuration = conf.getLong("stream.duration");

        SparkConf sparkConf = new SparkConf()
                .setAppName("Kafka Spark ES Flow with Java API")
                .setMaster(sparkMaster)
                .set("es.index.auto.create", "true")
                ;

        JavaSparkContext sp = new JavaSparkContext(sparkConf);
        JavaStreamingContext jssc = new JavaStreamingContext(sp, Durations.seconds(sparkStreamDuration));

        HashSet<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        HashMap<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", kafkaBrokers);
        kafkaParams.put("auto.offset.reset", fromOffset);

        // Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> messages = KafkaUtils
                .createDirectStream(jssc, String.class, String.class,
                        StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

        String esIndex = "rtb_test/log_item";
        String dictDir = "/media/sf_Download/ipinyou/dicts/";

        messages.foreachRDD(rdd -> {
            SQLContext sqlContext = SQLContext.getOrCreate(rdd.context());
            // AdExchange Dict
            DataFrame adExchangeDf = sqlContext.read().format("com.databricks.spark.csv")
                    .schema(AD_EXCH_SCHEMA.getSchema())
                    .option("header", "false")
                    .option("delimiter", "\t")
                    .load(Paths.get(dictDir, "ad.exchange.txt").toString());

            // Log Type Dict
            DataFrame logTypeDf = sqlContext.read().format("com.databricks.spark.csv")
                    .schema(LOG_TYPE_SCHEMA.getSchema())
                    .option("header", "false")
                    .option("delimiter", "\t")
                    .load(Paths.get(dictDir, "log.type.txt").toString());


            // City Dict
            DataFrame cityDf = sqlContext.read().format("com.databricks.spark.csv")
                    .schema(CITY_SCHEMA.getSchema())
                    .option("header", "false")
                    .option("delimiter", "\t")
                    .load(Paths.get(dictDir, "city.us.txt").toString());

            // US State Dict
            DataFrame stateDf = sqlContext.read().format("com.databricks.spark.csv")
                    .schema(STATE_SCHEMA.getSchema())
                    .option("header", "false")
                    .option("delimiter", "\t")
                    .load(Paths.get(dictDir, "states.us.txt").toString());


            JavaRDD<Row> rowRdd = rdd.map(x -> RowFactory.create(x._2().split("\t")));
            DataFrame bidDf = sqlContext.createDataFrame(rowRdd, BID_SCHEMA.getSchema());

            bidDf.show();

            DataFrame parsedBidDf = bidDf
                    .join(adExchangeDf, bidDf.col(AD_EXCH_ID.getStructField().name()).equalTo(adExchangeDf.col(AD_EXCH_ID.getStructField().name())), "left")
                    .join(logTypeDf, bidDf.col(LOG_TYPE_ID.getStructField().name()).equalTo(logTypeDf.col(LOG_TYPE_ID.getStructField().name())), "left")
                    .join(cityDf, bidDf.col(CITY_ID.getStructField().name()).equalTo(cityDf.col(CITY_ID.getStructField().name())), "inner")    // src data is messed a bit, so geo_point results in null -> 'inner' join is workaround
                    .join(stateDf, cityDf.col(STATE_ID.getStructField().name()).equalTo(stateDf.col(STATE_ID.getStructField().name())), "left")
                    .withColumn("user_tags_array", callUDF(new ParseUserTagsArray(), DataTypes.createArrayType(DataTypes.StringType), bidDf.col(USER_TAGS.getStructField().name())))
                    .withColumn("coordinates", callUDF(new ParseCoordinates(), DataTypes.createArrayType(DataTypes.FloatType),
                            cityDf.col(CITY_LATITUDE.getStructField().name()), cityDf.col(CITY_LONGITUDE.getStructField().name())))
                            // User Info
                            // -- browser
                    .withColumn("ua_browser", callUDF(new ParseUserAgentString("browser"), DataTypes.StringType, bidDf.col(USER_AGENT.getStructField().name())))
                    .withColumn("ua_browser_group", callUDF(new ParseUserAgentString("browser.group"), DataTypes.StringType, bidDf.col(USER_AGENT.getStructField().name())))
                    .withColumn("ua_browser_manufacturer", callUDF(new ParseUserAgentString("browser.manufacturer"), DataTypes.StringType, bidDf.col(USER_AGENT.getStructField().name())))
                    .withColumn("ua_browser_rendering_engine", callUDF(new ParseUserAgentString("browser.rendering.engine"), DataTypes.StringType, bidDf.col(USER_AGENT.getStructField().name())))
                    .withColumn("ua_browserVersion", callUDF(new ParseUserAgentString("browserVersion"), DataTypes.StringType, bidDf.col(USER_AGENT.getStructField().name())))
                    .withColumn("ua_browserVersion_minor", callUDF(new ParseUserAgentString("browserVersion.minor"), DataTypes.StringType, bidDf.col(USER_AGENT.getStructField().name())))
                    .withColumn("ua_browserVersion_major", callUDF(new ParseUserAgentString("browserVersion.major"), DataTypes.StringType, bidDf.col(USER_AGENT.getStructField().name())))
                            // -- id
                    .withColumn("ua_id", callUDF(new ParseUserAgentString("id"), DataTypes.StringType, bidDf.col(USER_AGENT.getStructField().name())))
                            // -- OS
                    .withColumn("ua_os", callUDF(new ParseUserAgentString("operatingSystem"), DataTypes.StringType, bidDf.col(USER_AGENT.getStructField().name())))
                    .withColumn("ua_os_name", callUDF(new ParseUserAgentString("operatingSystem.name"), DataTypes.StringType, bidDf.col(USER_AGENT.getStructField().name())))
                    .withColumn("ua_os_device", callUDF(new ParseUserAgentString("operatingSystem.device"), DataTypes.StringType, bidDf.col(USER_AGENT.getStructField().name())))
                    .withColumn("ua_os_group", callUDF(new ParseUserAgentString("operatingSystem.group"), DataTypes.StringType, bidDf.col(USER_AGENT.getStructField().name())))
                    .withColumn("ua_os_manufacturer", callUDF(new ParseUserAgentString("operatingSystem.manufacturer"), DataTypes.StringType, bidDf.col(USER_AGENT.getStructField().name())))
                    ;

            parsedBidDf.show();
//            JavaEsSpark.saveJsonToEs(parsedBidDf.toJSON().toJavaRDD(), esIndex);

            return null;
        });

        jssc.start();
        jssc.awaitTermination();

    }
}
