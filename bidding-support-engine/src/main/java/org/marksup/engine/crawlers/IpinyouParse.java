package org.marksup.engine.crawlers;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.marksup.engine.spark.sql.udf.ParseCoordinates;
import org.marksup.engine.spark.sql.udf.ParseUserAgentString;
import org.marksup.engine.spark.sql.udf.ParseUserTagsArray;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.apache.spark.sql.functions.callUDF;
import static org.marksup.engine.utils.MapperConstants.MappingSchemas.*;
import static org.marksup.engine.utils.MapperConstants.SchemaFields.*;

public class IpinyouParse {
    public static void main(String[] args) throws IOException {
        String esIndex = "rtb_test/log_item";
        String filePath = "/media/sf_Download/ipinyou/test";

        IpinyouParse parser = new IpinyouParse();
        parser.run(esIndex, filePath);
    }

    public void run(String esIndex, String filePath) throws IOException {
        String dictDir = "/media/sf_Download/ipinyou/dicts/";

        SparkConf conf = new SparkConf().setAppName("IpinyouParser").setMaster("local[4]")
        // .set("es.index.auto.create", "true")
        ;
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);

        // AdExchange Dict
        DataFrame adExchangeDf = sqlContext.read().format("com.databricks.spark.csv").schema(AD_EXCH_SCHEMA.getSchema())
                .option("header", "false").option("delimiter", "\t").load(Paths.get(dictDir, "ad.exchange.txt").toString());

        // Log Type Dict
        DataFrame logTypeDf = sqlContext.read().format("com.databricks.spark.csv").schema(LOG_TYPE_SCHEMA.getSchema())
                .option("header", "false").option("delimiter", "\t").load(Paths.get(dictDir, "log.type.txt").toString());

        // City Dict
        DataFrame cityDf = sqlContext.read().format("com.databricks.spark.csv").schema(CITY_SCHEMA.getSchema()).option("header", "false")
                .option("delimiter", "\t").load(Paths.get(dictDir, "city.us.txt").toString());

        // US State Dict
        DataFrame stateDf = sqlContext.read().format("com.databricks.spark.csv").schema(STATE_SCHEMA.getSchema()).option("header", "false")
                .option("delimiter", "\t").load(Paths.get(dictDir, "states.us.txt").toString());

        Files.walk(Paths.get(filePath)).forEach(fileName -> {
            if (Files.isRegularFile(fileName)) {
                System.out.println(fileName);

                DataFrame bidDf = sqlContext.read().format("com.databricks.spark.csv").schema(BID_SCHEMA.getSchema())
                        .option("header", "false").option("delimiter", "\t").load(fileName.toString());

                // Dynamic mapping + ES predefined (_timestamp, geo_point, ...)
                DataFrame parsedBidDf = bidDf
                        .join(adExchangeDf,
                                bidDf.col(AD_EXCH_ID.getStructField().name()).equalTo(adExchangeDf.col(AD_EXCH_ID.getStructField().name())),
                                "left")
                        .join(logTypeDf,
                                bidDf.col(LOG_TYPE_ID.getStructField().name()).equalTo(logTypeDf.col(LOG_TYPE_ID.getStructField().name())),
                                "left")
                        .join(cityDf, bidDf.col(CITY_ID.getStructField().name()).equalTo(cityDf.col(CITY_ID.getStructField().name())),
                                "inner") // src data is messed a bit, so geo_point results in null -> 'inner' join is workaround
                        .join(stateDf, cityDf.col(STATE_ID.getStructField().name()).equalTo(stateDf.col(STATE_ID.getStructField().name())),
                                "left")
                        .withColumn(USER_TAGS_ARRAY.getStructField().name(),
                                callUDF(new ParseUserTagsArray(), DataTypes.createArrayType(DataTypes.StringType),
                                        bidDf.col(USER_TAGS.getStructField().name())))
                        .withColumn(COORDINATES.getStructField().name(),
                                callUDF(new ParseCoordinates(), DataTypes.createArrayType(DataTypes.FloatType),
                                        cityDf.col(CITY_LATITUDE.getStructField().name()),
                                        cityDf.col(CITY_LONGITUDE.getStructField().name())))
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
                                callUDF(new ParseUserAgentString(UA_ID), DataTypes.StringType,
                                        bidDf.col(USER_AGENT.getStructField().name())))
                        // -- OS
                        .withColumn(UA_OS.getStructField().name(),
                                callUDF(new ParseUserAgentString(UA_OS), DataTypes.StringType,
                                        bidDf.col(USER_AGENT.getStructField().name())))
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

                JavaEsSpark.saveJsonToEs(parsedBidDf.toJSON().toJavaRDD(), esIndex);
            }
        });
    }
}
