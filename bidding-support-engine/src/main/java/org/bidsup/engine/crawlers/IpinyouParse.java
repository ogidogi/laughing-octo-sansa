package org.bidsup.engine.crawlers;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.bidsup.engine.spark.sql.udf.ParseCoordinates;
import org.bidsup.engine.spark.sql.udf.ParseUserAgentString;
import org.bidsup.engine.spark.sql.udf.ParseUserTagsArray;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.apache.spark.sql.functions.callUDF;
import static org.bidsup.engine.utils.MapperConstants.MappingSchemas.*;
import static org.bidsup.engine.utils.MapperConstants.SchemaFields.*;


public class IpinyouParse {
    public static void main(String[] args) throws IOException {
        String esIndex = "rtb_test/log_item";
        String filePath = "/media/sf_Download/ipinyou/test";

        IpinyouParse parser = new IpinyouParse();
        parser.run(esIndex, filePath);
    }

    public void run(String esIndex, String filePath) throws IOException {
        String dictDir = "/media/sf_Download/ipinyou/dicts/";

        SparkConf conf = new SparkConf()
                .setAppName("IpinyouParser")
                .setMaster("local[4]")
                //                .set("es.index.auto.create", "true")
                ;
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);


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

        Files.walk(Paths.get(filePath)).forEach(fileName -> {
            if (Files.isRegularFile(fileName)) {
                System.out.println(fileName);

                DataFrame bidDf = sqlContext.read().format("com.databricks.spark.csv")
                        .schema(BID_SCHEMA.getSchema())
                        .option("header", "false")
                        .option("delimiter", "\t")
                        .load(fileName.toString());

                // Dynamic mapping + ES predefined (_timestamp, geo_point, ...)
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

                JavaEsSpark.saveJsonToEs(parsedBidDf.toJSON().toJavaRDD(), esIndex);
            }
        });
    }
}




