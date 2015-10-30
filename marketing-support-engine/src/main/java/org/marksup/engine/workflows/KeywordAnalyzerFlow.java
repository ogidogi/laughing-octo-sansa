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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.marksup.engine.spark.sql.udf.ParseCoordinates;
import org.marksup.engine.spark.sql.udf.ParseUserAgentString;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.coalesce;

import static org.marksup.engine.utils.MapperConstants.MappingSchemas.*;
import static org.marksup.engine.utils.MapperConstants.SchemaFields.*;


public class KeywordAnalyzerFlow {
    private static final Logger log = Logger.getLogger(KeywordAnalyzerFlow.class);
    private static final String dictDir = "/media/sf_Download/data/mors/new_dicts";

    public static void main(String[] args) throws ConfigurationException {
        KeywordAnalyzerFlow workflow = new KeywordAnalyzerFlow();
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

    public void run(CompositeConfiguration conf) throws IOException, ClassNotFoundException {
        String esIndexName = "keyword/log";

        // Kafka props
        String kafkaBrokers = conf.getString("metadata.broker.list");
        String topics = conf.getString("consumer.topic");
        String fromOffset = conf.getString("auto.offset.reset");

        // Spark props
        String sparkMaster = conf.getString("spark.master");
        long sparkStreamDuration = conf.getLong("stream.duration");

        SparkConf sparkConf = new SparkConf().setAppName("KEYWORD_FLOW").setMaster(sparkMaster)
                .set("es.index.auto.create", conf.getString("es.index.auto.create"));

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(sparkStreamDuration));
        SQLContext sqlCon = SQLContext.getOrCreate(jsc.sc());

        HashSet<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        HashMap<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", kafkaBrokers);
        kafkaParams.put("auto.offset.reset", fromOffset);

        // Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class,
                StringDecoder.class, kafkaParams, topicsSet);
//        messages.print();

        log.debug("Create Data Frames dictionaries from CSV");
        boolean isDictHeader = true;
        DataFrame adExchangeDf = BiddingLogWorkflow.getDataFrameFromCsv(sqlCon, AD_EXCH_SCHEMA, Paths.get(dictDir, "ad.exchange.txt"),
                isDictHeader);
        DataFrame logTypeDf = BiddingLogWorkflow.getDataFrameFromCsv(sqlCon, LOG_TYPE_SCHEMA, Paths.get(dictDir, "log.type.txt"),
                isDictHeader);
        DataFrame cityDf = BiddingLogWorkflow.getDataFrameFromCsv(sqlCon, CITY_SCHEMA, Paths.get(dictDir, "city.us.txt"), isDictHeader);
        DataFrame stateDf = BiddingLogWorkflow.getDataFrameFromCsv(sqlCon, STATE_SCHEMA, Paths.get(dictDir, "states.us.txt"), isDictHeader);
        DataFrame sitePageDf = BiddingLogWorkflow.getDataFrameFromCsv(sqlCon, SITE_PAGES_SCHEMA, Paths.get(dictDir, "site.pages.us.txt"),
                isDictHeader);
        DataFrame userProfileTagDf = BiddingLogWorkflow.getDataFrameFromCsv(sqlCon, USER_PROFILE_TAGS_SCHEMA,
                Paths.get(dictDir, "user.profile.tags.us.txt"), isDictHeader);

        log.debug("Create Keyword dictionary");
        DataFrame keywordDf = sitePageDf.select(sitePageDf.col(SITE_PAGE_ID.getName()), sitePageDf.col(SITE_PAGE_TAG.getName()))
                .unionAll(userProfileTagDf.select(userProfileTagDf.col(USER_PROFILE_TAG_ID.getName()),
                        userProfileTagDf.col(USER_PROFILE_TAG_VALUE.getName())))
                .withColumnRenamed(SITE_PAGE_ID.getName(), KEYWORD_ID.getName())
                .withColumnRenamed(SITE_PAGE_TAG.getName(), KEYWORD_NAME.getName());

        // Persist data
        adExchangeDf.persist();
        logTypeDf.persist();
        cityDf.persist();
        stateDf.persist();
        sitePageDf.persist();
        userProfileTagDf.persist();
        keywordDf.persist();

        messages.foreachRDD(rdd -> {
            SQLContext sqlContext = SQLContext.getOrCreate(rdd.context());
            JavaRDD<Row> rowRdd = rdd.filter(x -> !x._2().isEmpty()).map(x -> BID_LOG_SCHEMA.textToRow(x._2(), "\t"));

            if (rowRdd.isEmpty()) {
                return null;
            }

            DataFrame kafkaDf = sqlContext.createDataFrame(rowRdd.rdd(), BID_LOG_SCHEMA.getSchema(), true);
            kafkaDf.persist();
            kafkaDf.show();

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

            joinedKafkaDf.persist();
            joinedKafkaDf.show();

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
                            callUDF(new ParseUserAgentString(UA_BROWSER), UA_BROWSER.getStructField().dataType(),
                                    kafkaDf.col(USER_AGENT.getName())))
                    .withColumn(UA_BROWSER_GROUP.getName(),
                            callUDF(new ParseUserAgentString(UA_BROWSER_GROUP), UA_BROWSER_GROUP.getStructField().dataType(),
                                    kafkaDf.col(USER_AGENT.getName())))
                    .withColumn(UA_BROWSER_MANUFACTURER.getName(),
                            callUDF(new ParseUserAgentString(UA_BROWSER_MANUFACTURER), UA_BROWSER_MANUFACTURER.getStructField().dataType(),
                                    kafkaDf.col(USER_AGENT.getName())))
                    .withColumn(UA_BROWSER_RENDERING_ENGINE.getName(),
                            callUDF(new ParseUserAgentString(UA_BROWSER_RENDERING_ENGINE),
                                    UA_BROWSER_RENDERING_ENGINE.getStructField().dataType(), kafkaDf.col(USER_AGENT.getName())))
                    .withColumn(UA_BROWSERVERSION.getName(),
                            callUDF(new ParseUserAgentString(UA_BROWSERVERSION), UA_BROWSERVERSION.getStructField().dataType(),
                                    kafkaDf.col(USER_AGENT.getName())))
                    .withColumn(UA_BROWSERVERSION_MINOR.getName(),
                            callUDF(new ParseUserAgentString(UA_BROWSERVERSION_MINOR), UA_BROWSERVERSION_MINOR.getStructField().dataType(),
                                    kafkaDf.col(USER_AGENT.getName())))
                    .withColumn(UA_BROWSERVERSION_MAJOR.getName(),
                            callUDF(new ParseUserAgentString(UA_BROWSERVERSION_MAJOR), UA_BROWSERVERSION_MAJOR.getStructField().dataType(),
                                    kafkaDf.col(USER_AGENT.getName())))
                    .withColumn(UA_ID.getName(),
                            callUDF(new ParseUserAgentString(UA_ID), UA_ID.getStructField().dataType(), kafkaDf.col(USER_AGENT.getName())))
                    .withColumn(UA_OS.getName(),
                            callUDF(new ParseUserAgentString(UA_OS), UA_OS.getStructField().dataType(), kafkaDf.col(USER_AGENT.getName())))
                    .withColumn(UA_OS_NAME.getName(),
                            callUDF(new ParseUserAgentString(UA_OS_NAME), UA_OS_NAME.getStructField().dataType(),
                                    kafkaDf.col(USER_AGENT.getName())))
                    .withColumn(UA_OS_DEVICE.getName(),
                            callUDF(new ParseUserAgentString(UA_OS_DEVICE), UA_OS_DEVICE.getStructField().dataType(),
                                    kafkaDf.col(USER_AGENT.getName())))
                    .withColumn(UA_OS_GROUP.getName(),
                            callUDF(new ParseUserAgentString(UA_OS_GROUP), UA_OS_GROUP.getStructField().dataType(),
                                    kafkaDf.col(USER_AGENT.getName())))
                    .withColumn(UA_OS_MANUFACTURER.getName(), callUDF(new ParseUserAgentString(UA_OS_MANUFACTURER),
                            UA_OS_MANUFACTURER.getStructField().dataType(), kafkaDf.col(USER_AGENT.getName())));
            searchCompatibleDf.persist();
            searchCompatibleDf.show();

            if (searchCompatibleDf.count() > 0) {
                log.info(String.format("Saving to ES %s", esIndexName));
                JavaEsSpark.saveJsonToEs(searchCompatibleDf.toJSON().toJavaRDD(), esIndexName);
            }

            return null;
        });

        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }


}

