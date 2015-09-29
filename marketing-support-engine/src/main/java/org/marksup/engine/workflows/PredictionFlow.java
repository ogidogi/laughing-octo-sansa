package org.marksup.engine.workflows;

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

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.coalesce;
import static org.marksup.engine.utils.MapperConstants.MappingSchemas.*;
import static org.marksup.engine.utils.MapperConstants.SchemaFields.*;

public class PredictionFlow {
    private static final Logger log = Logger.getLogger(PredictionFlow.class);
    private static final String dictDir = "/media/sf_Download/data/mors/new_dicts";


    public static void main(String[] args) throws ConfigurationException {
        PredictionFlow workflow = new PredictionFlow();
        log.setLevel(Level.DEBUG);

        CompositeConfiguration conf = new CompositeConfiguration();
        conf.addConfiguration(new PropertiesConfiguration("kafka.properties"));
        conf.addConfiguration(new PropertiesConfiguration("spark.properties"));
        conf.addConfiguration(new PropertiesConfiguration("cassandra.properties"));

        try {
            workflow.run(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void run(CompositeConfiguration conf) {
        // Kafka props
        String kafkaBrokers = conf.getString("metadata.broker.list");
        String topics = conf.getString("consumer.topic");
        String fromOffset = conf.getString("auto.offset.reset");

        // Spark props
        String sparkMaster = conf.getString("spark.master");
        long sparkStreamDuration = conf.getLong("stream.duration");

        // Cassandra props
        String cassandraKeyspace = "test";
        String cassandraTable = "kafka_logstream";
        String cassandraDbNode = conf.getString("cassandra.database.node");

        HashMap<String, String> cassandraParams = new HashMap<>();
        cassandraParams.put("table", "predict_kafka_raw");
        cassandraParams.put("keyspace", "test");

        SparkConf sparkConf = new SparkConf().setAppName("PREDICTION_FLOW").setMaster(sparkMaster)
                .set("spark.cassandra.connection.host", cassandraDbNode);

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

//        SQLContext sqlContext = new SQLContext(jsc);
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(sparkStreamDuration));

        HashSet<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        HashMap<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", kafkaBrokers);
        kafkaParams.put("auto.offset.reset", fromOffset);

        // Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class,
                StringDecoder.class, kafkaParams, topicsSet);

        messages.print();

        messages.foreachRDD(rdd -> {
            SQLContext sqlContext = SQLContext.getOrCreate(rdd.context());
            JavaRDD<Row> rowRdd = rdd.filter(x -> !x._2().isEmpty()).map(x -> new GenericRow(x._2().split("\t")));

            if (rowRdd.isEmpty()) {
                return null;
            }

            log.debug("Create Data Frames dictionaries from CSV");

            boolean isDictHeader = true;
            DataFrame adExchangeDf = BiddingLogWorkflow.getDataFrameFromCsv(sqlContext, AD_EXCH_SCHEMA, Paths.get(dictDir, "ad.exchange.txt"),
                    isDictHeader);
            DataFrame logTypeDf = BiddingLogWorkflow.getDataFrameFromCsv(sqlContext, LOG_TYPE_SCHEMA, Paths.get(dictDir, "log.type.txt"),
                    isDictHeader);
            DataFrame cityDf = BiddingLogWorkflow.getDataFrameFromCsv(sqlContext, CITY_SCHEMA, Paths.get(dictDir, "city.us.txt"), isDictHeader);
            DataFrame stateDf = BiddingLogWorkflow.getDataFrameFromCsv(sqlContext, STATE_SCHEMA, Paths.get(dictDir, "states.us.txt"),
                    isDictHeader);
            DataFrame sitePageDf = BiddingLogWorkflow.getDataFrameFromCsv(sqlContext, SITE_PAGES_SCHEMA,
                    Paths.get(dictDir, "site.pages.us.txt"), isDictHeader);
            DataFrame userProfileTagDf = BiddingLogWorkflow.getDataFrameFromCsv(sqlContext, USER_PROFILE_TAGS_SCHEMA,
                    Paths.get(dictDir, "user.profile.tags.us.txt"), isDictHeader);

            log.debug("Create Keyword dictionary");
            DataFrame keywordDf = sitePageDf.select(sitePageDf.col(SITE_PAGE_ID.getName()), sitePageDf.col(SITE_PAGE_TAG.getName()))
                    .unionAll(userProfileTagDf.select(userProfileTagDf.col(USER_PROFILE_TAG_ID.getName()),
                            userProfileTagDf.col(USER_PROFILE_TAG_VALUE.getName())))
                    .withColumnRenamed(SITE_PAGE_ID.getName(), KEYWORD_ID.getName())
                    .withColumnRenamed(SITE_PAGE_TAG.getName(), KEYWORD_NAME.getName());


            // Workaround since there is no automatic type conversion between Row and DF
            StructType kafkaStrSchema = DataTypes.createStructType(Arrays.stream(BID_LOG_SCHEMA.getSchema().fields())
                    .map(field -> DataTypes.createStructField(field.name(), DataTypes.StringType, field.nullable()))
                    .collect(Collectors.toList()));

            DataFrame kafkaDf = sqlContext.createDataFrame(rowRdd, kafkaStrSchema);
//            kafkaDf.show();

            log.info("Join with dictionaries");
            DataFrame joinedKafkaDf = kafkaDf
                    .join(adExchangeDf, kafkaDf.col(AD_EXCH_ID.getName()).equalTo(adExchangeDf.col(AD_EXCH_ID.getName())), "left")
                    .join(logTypeDf, kafkaDf.col(LOG_TYPE_ID.getName()).equalTo(logTypeDf.col(LOG_TYPE_ID.getName())), "left")
                    .join(cityDf, kafkaDf.col(CITY_ID.getName()).equalTo(cityDf.col(CITY_ID.getName())), "inner") // src data is messed a bit, so geo_point results in null -> 'inner' join is
                            // workaround
                    .join(stateDf, cityDf.col(STATE_ID.getName()).equalTo(stateDf.col(STATE_ID.getName())), "left")
                    .join(keywordDf, kafkaDf.col(USER_TAGS.getName()).equalTo(keywordDf.col(KEYWORD_ID.getName())), "left")
                    .withColumn(COORDINATES.getName(), callUDF(new ParseCoordinates(), DataTypes.createArrayType(DataTypes.FloatType),
                            cityDf.col(CITY_LATITUDE.getName()), cityDf.col(CITY_LONGITUDE.getName())));

//            joinedKafkaDf.show();

            DataFrame searchCompatibleDf = joinedKafkaDf
                    .select(
                            // BID
                            joinedKafkaDf.col(BID_ID.getName()), joinedKafkaDf.col(TIMESTAMP.getName()),
                            joinedKafkaDf.col(IPINYOU_ID.getName()), joinedKafkaDf.col(USER_AGENT.getName()),
                            joinedKafkaDf.col(IP.getName()), joinedKafkaDf.col(DOMAIN.getName()), joinedKafkaDf.col(URL.getName()),
                            joinedKafkaDf.col(ANONYMOUS_URL_ID.getName()), joinedKafkaDf.col(AD_SLOT_ID.getName()),
                            joinedKafkaDf.col(AD_SLOT_WIDTH.getName()), joinedKafkaDf.col(AD_SLOT_HEIGHT.getName()),
                            joinedKafkaDf.col(AD_SLOT_VISIBILITY.getName()), joinedKafkaDf.col(AD_SLOT_FORMAT.getName()),
                            joinedKafkaDf.col(AD_SLOT_FLOOR_PRICE.getName()), joinedKafkaDf.col(CREATIVE_ID.getName()),
                            joinedKafkaDf.col(BIDDING_PRICE.getName()), joinedKafkaDf.col(ADVERTISER_ID.getName()),
                            joinedKafkaDf.col(PAYING_PRICE.getName()),
                            // ADX
                            joinedKafkaDf.col(AD_EXCH_NAME.getName()), joinedKafkaDf.col(AD_EXCH_DESC.getName()),
                            // LOG TYPE
                            joinedKafkaDf.col(LOG_TYPE_NAME.getName()),
                            // CITY
                            joinedKafkaDf.col(CITY_NAME.getName()), joinedKafkaDf.col(CITY_POPULATION.getName()),
                            joinedKafkaDf.col(CITY_AREA.getName()), joinedKafkaDf.col(CITY_DENSITY.getName()),
                            joinedKafkaDf.col(COORDINATES.getName()),
                            // STATE
                            joinedKafkaDf.col(STATE_NAME.getName()), joinedKafkaDf.col(STATE_POPULATION.getName()),
                            joinedKafkaDf.col(STATE_GSP.getName()),
                            // KEYWORD
                            coalesce(joinedKafkaDf.col(KEYWORD_NAME.getName()), joinedKafkaDf.col(USER_TAGS.getName()))
                                    .alias(KEYWORD_NAME.getName()))
                    .withColumn(UA_BROWSER.getName(),
                            callUDF(new ParseUserAgentString(UA_BROWSER), DataTypes.StringType, kafkaDf.col(USER_AGENT.getName())))
                    .withColumn(UA_BROWSER_GROUP.getName(),
                            callUDF(new ParseUserAgentString(UA_BROWSER_GROUP), DataTypes.StringType,
                                    kafkaDf.col(USER_AGENT.getName())))
                    .withColumn(UA_BROWSER_MANUFACTURER.getName(),
                            callUDF(new ParseUserAgentString(UA_BROWSER_MANUFACTURER), DataTypes.StringType,
                                    kafkaDf.col(USER_AGENT.getName())))
                    .withColumn(UA_BROWSER_RENDERING_ENGINE.getName(),
                            callUDF(new ParseUserAgentString(UA_BROWSER_RENDERING_ENGINE), DataTypes.StringType,
                                    kafkaDf.col(USER_AGENT.getName())))
                    .withColumn(UA_BROWSERVERSION.getName(),
                            callUDF(new ParseUserAgentString(UA_BROWSERVERSION), DataTypes.StringType,
                                    kafkaDf.col(USER_AGENT.getName())))
                    .withColumn(UA_BROWSERVERSION_MINOR.getName(),
                            callUDF(new ParseUserAgentString(UA_BROWSERVERSION_MINOR), DataTypes.StringType,
                                    kafkaDf.col(USER_AGENT.getName())))
                    .withColumn(UA_BROWSERVERSION_MAJOR.getName(),
                            callUDF(new ParseUserAgentString(UA_BROWSERVERSION_MAJOR), DataTypes.StringType,
                                    kafkaDf.col(USER_AGENT.getName())))
                    .withColumn(UA_ID.getName(),
                            callUDF(new ParseUserAgentString(UA_ID), DataTypes.StringType, kafkaDf.col(USER_AGENT.getName())))
                    .withColumn(UA_OS.getName(),
                            callUDF(new ParseUserAgentString(UA_OS), DataTypes.StringType, kafkaDf.col(USER_AGENT.getName())))
                    .withColumn(UA_OS_NAME.getName(),
                            callUDF(new ParseUserAgentString(UA_OS_NAME), DataTypes.StringType, kafkaDf.col(USER_AGENT.getName())))
                    .withColumn(UA_OS_DEVICE.getName(),
                            callUDF(new ParseUserAgentString(UA_OS_DEVICE), DataTypes.StringType, kafkaDf.col(USER_AGENT.getName())))
                    .withColumn(UA_OS_GROUP.getName(),
                            callUDF(new ParseUserAgentString(UA_OS_GROUP), DataTypes.StringType, kafkaDf.col(USER_AGENT.getName())))
                    .withColumn(UA_OS_MANUFACTURER.getName(), callUDF(new ParseUserAgentString(UA_OS_MANUFACTURER),
                            DataTypes.StringType, kafkaDf.col(USER_AGENT.getName())));

            log.debug("Parsed DF");
            searchCompatibleDf.show();

            DataFrame cassandraDf = sqlContext
                    .read()
                    .format("org.apache.spark.sql.cassandra")
                    .options(cassandraParams)
                    .load();

            log.debug("Cassandra DF");
            cassandraDf.show();

            log.debug("Joined DF");
            DataFrame sqlTblDf = searchCompatibleDf
                    .join(cassandraDf, searchCompatibleDf.col(IPINYOU_ID.getName()).equalTo(cassandraDf.col("ipinyou_id")), "leftouter")
                    .filter(searchCompatibleDf.col("timestamp").geq(cassandraDf.col("timestamp")))
                    .select(
                            searchCompatibleDf.col("ipinyou_id"),
//                            searchCompatibleDf.col("*"),
                            cassandraDf.col("timestamp").as("cass_ts"),
                            cassandraDf.col("keyword_name").as("cass_kw_name"),
                            cassandraDf.col("log_type_name").as("cass_log_type_name")
                    )
                    ;

            sqlTblDf.show();

            sqlTblDf.registerTempTable("unpivoted_table");
            DataFrame df = sqlContext.sql(
                    " select ipinyou_id,max(bid_click_kw) bid_click_kw,max(site_open_kw) site_open_kw," +
                            " max(site_search_kw) site_search_kw,max(site_click_kw) site_click_kw, max(site_lead_kw) site_lead_kw" +
                            " from (" +
                            " select ipinyou_id," +
                            " case when cass_log_type_name='bid-click' then cass_kw_name else 0 end bid_click_kw," +
                            " case when cass_log_type_name='site-open' then cass_kw_name else 0 end site_open_kw," +
                            " case when cass_log_type_name='site-search' then cass_kw_name else 0 end site_search_kw," +
                            " case when cass_log_type_name='site-click' then cass_kw_name else 0 end site_click_kw," +
                            " case when cass_log_type_name='site-lead' then cass_kw_name else 0 end site_lead_kw" +
                            " from unpivoted_table) x" +
                            " group by ipinyou_id");

            df.show();

            DataFrame forPredictDf = searchCompatibleDf
                    .join(df, searchCompatibleDf.col("ipinyou_id").equalTo(df.col("ipinyou_id")), "leftouter")
                    .drop(df.col("ipinyou_id"))
                    ;

            log.debug("Data for prediction");
            forPredictDf.show();

//            if (searchCompatibleDf.count() > 0) {
//                log.info("Load to Cassandra");
//                searchCompatibleDf
//                        .write()
//                        .format("org.apache.spark.sql.cassandra")
//                        .options(cassandraParams)
//                        .mode(SaveMode.Overwrite)
//                        .save();
//            }

            return null;
        });

        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }
}
