package org.bidsup.engine.workflows;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.bidsup.engine.spark.sql.udf.ParseCoordinates;
import org.bidsup.engine.spark.sql.udf.ParseUserAgentString;
import org.bidsup.engine.utils.MapperConstants;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.spark.sql.functions.callUDF;
import static org.bidsup.engine.utils.MapperConstants.MappingSchemas.*;
import static org.bidsup.engine.utils.MapperConstants.SchemaFields.*;

public class BiddingLogWorkflow {
    private static final Logger log = Logger.getLogger(BiddingLogWorkflow.class);
    private static final String dictDir = "/media/sf_Download/ipinyou/new_dicts";

    public static void main(String[] args) throws ConfigurationException {
        log.setLevel(Level.DEBUG);

        CompositeConfiguration conf = new CompositeConfiguration();
        conf.addConfiguration(new PropertiesConfiguration("kafka.properties"));
        conf.addConfiguration(new PropertiesConfiguration("spark.properties"));
        conf.addConfiguration(new PropertiesConfiguration("es.properties"));

        Path filePath = Paths.get("/media/sf_Download/ipinyou/new_test");
        String esIdxSuffix = "log_%s/bid";

        BiddingLogWorkflow wf = new BiddingLogWorkflow();

        try {
            wf.runBatch(conf, filePath, esIdxSuffix);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static DataFrame getDataFrameFromCsv(SQLContext sqlContext, MapperConstants.MappingSchemas schema, Path filePath, Boolean isHeader) {
        return sqlContext.read().format("com.databricks.spark.csv")
                .schema(schema.getSchema())
                .option("header", isHeader.toString())
                .option("delimiter", "\t")
                .load(filePath.toString());
    }

    public static String getDateFromName(String fileName) throws IllegalArgumentException {
        // e.g.: site-click.20130606-aa.txt
        Matcher matcher = Pattern.compile("\\d{8}").matcher(fileName);
        if (matcher.find()) {
            return matcher.group();
        } else {
            throw new IllegalArgumentException("Can't parse '%s' to date format representation. Use YYYYMMDD");
        }
    }

    public static String getEsIdxFromName(String fileName, String esIdxSuffix) {
        String datePart = BiddingLogWorkflow.getDateFromName(fileName);
        return String.format(esIdxSuffix, datePart);
    }

    public void runBatch(CompositeConfiguration conf, Path filePath, String esIdxSuffix) throws IOException {
        SparkConf sparkConf = new SparkConf()
            .setAppName("BATCH_BIDDING_LOG_STORE_TO_ES")
            .setMaster(conf.getString("spark.master"))
            .set("spark.serializer", conf.getString("spark.serializer"))
            .set("es.index.auto.create", conf.getString("es.index.auto.create"));

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(jsc);

        log.debug("Create Data Frames dictionaries from CSV");

        boolean isDictHeader = true;
        DataFrame adExchangeDf = BiddingLogWorkflow.getDataFrameFromCsv(sqlContext, AD_EXCH_SCHEMA, Paths.get(dictDir, "ad.exchange.txt"), isDictHeader);
        DataFrame logTypeDf = BiddingLogWorkflow.getDataFrameFromCsv(sqlContext, LOG_TYPE_SCHEMA, Paths.get(dictDir, "log.type.txt"), isDictHeader);
        DataFrame cityDf = BiddingLogWorkflow.getDataFrameFromCsv(sqlContext, CITY_SCHEMA, Paths.get(dictDir, "city.us.txt"), isDictHeader);
        DataFrame stateDf = BiddingLogWorkflow.getDataFrameFromCsv(sqlContext, STATE_SCHEMA, Paths.get(dictDir, "states.us.txt"), isDictHeader);
        DataFrame sitePageDf = BiddingLogWorkflow.getDataFrameFromCsv(sqlContext, SITE_PAGES_SCHEMA, Paths.get(dictDir, "site.pages.us.txt"), isDictHeader);
        DataFrame userProfileTagDf = BiddingLogWorkflow.getDataFrameFromCsv(sqlContext, USER_PROFILE_TAGS_SCHEMA, Paths.get(dictDir, "user.profile.tags.us.txt"), isDictHeader);

        log.debug("Create Keyword dictionary");
        DataFrame keywordDf = sitePageDf.select(
                sitePageDf.col(SITE_PAGE_ID.getName()),
                sitePageDf.col(SITE_PAGE_TAG.getName())
            )
            .unionAll(userProfileTagDf.select(
                userProfileTagDf.col(USER_PROFILE_TAG_ID.getName()),
                userProfileTagDf.col(USER_PROFILE_TAG_VALUE.getName())
                )
            )
            .withColumnRenamed(SITE_PAGE_ID.getName(), KEYWORD_ID.getName())
            .withColumnRenamed(SITE_PAGE_TAG.getName(), KEYWORD_VALUE.getName());

        // Walk through dir and process log files
        // put files in ES index by date
        Files.walk(filePath).forEach(file -> {
            if (Files.isRegularFile(file)) {
                String fileName = file.getFileName().toString();
                log.info(String.format("Processing %s", fileName));
                String esIdx = BiddingLogWorkflow.getEsIdxFromName(fileName, esIdxSuffix);

                DataFrame bidLogDf = BiddingLogWorkflow.getDataFrameFromCsv(sqlContext, BID_LOG_SCHEMA, filePath, false);
                DataFrame parsedBidLogDf = bidLogDf
                    .join(adExchangeDf, bidLogDf.col(AD_EXCH_ID.getName()).equalTo(adExchangeDf.col(AD_EXCH_ID.getName())), "left")
                    .join(logTypeDf, bidLogDf.col(LOG_TYPE_ID.getName()).equalTo(logTypeDf.col(LOG_TYPE_ID.getName())), "left")
                    .join(cityDf, bidLogDf.col(CITY_ID.getName()).equalTo(cityDf.col(CITY_ID.getName())), "inner")    // src data is messed a bit, so geo_point results in null -> 'inner' join is workaround
                    .join(stateDf, cityDf.col(STATE_ID.getName()).equalTo(stateDf.col(STATE_ID.getName())), "left")
                    .join(keywordDf, bidLogDf.col(USER_TAG_ID.getName()).equalTo(keywordDf.col(KEYWORD_ID.getName())), "left")
                    .withColumn(COORDINATES.getName(), callUDF(new ParseCoordinates(), DataTypes.createArrayType(DataTypes.FloatType), cityDf.col(CITY_LATITUDE.getName()), cityDf.col(CITY_LONGITUDE.getName())))
                            // User Info
                            // -- browser
                    .withColumn(UA_BROWSER.getName(), callUDF(new ParseUserAgentString(UA_BROWSER), DataTypes.StringType, bidLogDf.col(USER_AGENT.getName())))
                    .withColumn(UA_BROWSER_GROUP.getName(), callUDF(new ParseUserAgentString(UA_BROWSER_GROUP), DataTypes.StringType, bidLogDf.col(USER_AGENT.getName())))
                    .withColumn(UA_BROWSER_MANUFACTURER.getName(), callUDF(new ParseUserAgentString(UA_BROWSER_MANUFACTURER), DataTypes.StringType, bidLogDf.col(USER_AGENT.getName())))
                    .withColumn(UA_BROWSER_RENDERING_ENGINE.getName(), callUDF(new ParseUserAgentString(UA_BROWSER_RENDERING_ENGINE), DataTypes.StringType, bidLogDf.col(USER_AGENT.getName())))
                    .withColumn(UA_BROWSERVERSION.getName(), callUDF(new ParseUserAgentString(UA_BROWSERVERSION), DataTypes.StringType, bidLogDf.col(USER_AGENT.getName())))
                    .withColumn(UA_BROWSERVERSION_MINOR.getName(), callUDF(new ParseUserAgentString(UA_BROWSERVERSION_MINOR), DataTypes.StringType, bidLogDf.col(USER_AGENT.getName())))
                    .withColumn(UA_BROWSERVERSION_MAJOR.getName(), callUDF(new ParseUserAgentString(UA_BROWSERVERSION_MAJOR), DataTypes.StringType, bidLogDf.col(USER_AGENT.getName())))
                            // -- id
                    .withColumn(UA_ID.getName(), callUDF(new ParseUserAgentString(UA_ID), DataTypes.StringType, bidLogDf.col(USER_AGENT.getName())))
                            // -- OS
                    .withColumn(UA_OS.getName(), callUDF(new ParseUserAgentString(UA_OS), DataTypes.StringType, bidLogDf.col(USER_AGENT.getName())))
                    .withColumn(UA_OS_NAME.getName(), callUDF(new ParseUserAgentString(UA_OS_NAME), DataTypes.StringType, bidLogDf.col(USER_AGENT.getName())))
                    .withColumn(UA_OS_DEVICE.getName(), callUDF(new ParseUserAgentString(UA_OS_DEVICE), DataTypes.StringType, bidLogDf.col(USER_AGENT.getName())))
                    .withColumn(UA_OS_GROUP.getName(), callUDF(new ParseUserAgentString(UA_OS_GROUP), DataTypes.StringType, bidLogDf.col(USER_AGENT.getName())))
                    .withColumn(UA_OS_MANUFACTURER.getName(), callUDF(new ParseUserAgentString(UA_OS_MANUFACTURER), DataTypes.StringType, bidLogDf.col(USER_AGENT.getName())))

                    .drop(bidLogDf.col(AD_EXCH_ID.getName()))
                    .drop(bidLogDf.col(LOG_TYPE_ID.getName()))
                    .drop(bidLogDf.col(CITY_ID.getName()))
                    .drop(cityDf.col(STATE_ID.getName()))
                    .drop(bidLogDf.col(USER_TAG_ID.getName()));

                parsedBidLogDf.show();
            }
        });
    }
}
