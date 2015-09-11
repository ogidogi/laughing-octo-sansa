package org.bidsup.engine.crawlers;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bidsup.engine.spark.sql.udf.ParseCoordinates;
import org.bidsup.engine.spark.sql.udf.ParseUserAgentString;
import org.bidsup.engine.spark.sql.udf.ParseUserTagsArray;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.callUDF;


public class IpinyouParse {
    public static void main(String[] args) throws IOException {
        String esIndex = "rtb/log_item";
        String filePath = "/media/sf_Download/ipinyou/";

        IpinyouParse parser = new IpinyouParse();
        parser.run(esIndex, filePath);
    }

    public void run(String esIndex, String filePath) throws IOException {
        String dictDir = "/media/sf_Download/ipinyou/dicts/";

        SparkConf conf = new SparkConf()
                .setAppName("IpinyouParser")
                .setMaster("local")
                .set("es.index.auto.create", "true")
                ;
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);

        List<StructField> bidFields = new ArrayList<StructField>();
        bidFields.add(DataTypes.createStructField("bid_id", DataTypes.StringType, true));
        bidFields.add(DataTypes.createStructField("timestamp", DataTypes.StringType, true));
        bidFields.add(DataTypes.createStructField("log_type", DataTypes.IntegerType, true));
        bidFields.add(DataTypes.createStructField("ipinyou_id", DataTypes.StringType, true));
        bidFields.add(DataTypes.createStructField("user_agent", DataTypes.StringType, true));
        bidFields.add(DataTypes.createStructField("ip", DataTypes.StringType, true));
        bidFields.add(DataTypes.createStructField("region", DataTypes.IntegerType, true));
        bidFields.add(DataTypes.createStructField("city", DataTypes.IntegerType, true));
        bidFields.add(DataTypes.createStructField("ad_exchange", DataTypes.IntegerType, true));
        bidFields.add(DataTypes.createStructField("domain", DataTypes.StringType, true));
        bidFields.add(DataTypes.createStructField("url", DataTypes.StringType, true));
        bidFields.add(DataTypes.createStructField("anonymous_url_id", DataTypes.StringType, true));
        bidFields.add(DataTypes.createStructField("ad_slot_id", DataTypes.StringType, true));
        bidFields.add(DataTypes.createStructField("ad_slot_width", DataTypes.IntegerType, true));
        bidFields.add(DataTypes.createStructField("ad_slot_height", DataTypes.IntegerType, true));
        bidFields.add(DataTypes.createStructField("ad_slot_visibility", DataTypes.StringType, true));
        bidFields.add(DataTypes.createStructField("ad_slot_format", DataTypes.StringType, true));
        bidFields.add(DataTypes.createStructField("ad_slot_floor_price", DataTypes.LongType, true));
        bidFields.add(DataTypes.createStructField("creative_id", DataTypes.StringType, true));
        bidFields.add(DataTypes.createStructField("bidding_price", DataTypes.FloatType, true));
        bidFields.add(DataTypes.createStructField("paying_price", DataTypes.FloatType, true));
        bidFields.add(DataTypes.createStructField("key_page_url", DataTypes.StringType, true));
        bidFields.add(DataTypes.createStructField("advertiser_id", DataTypes.LongType, true));
        bidFields.add(DataTypes.createStructField("user_tags", DataTypes.StringType, true));
        //        bidFields.add(DataTypes.createStructField("user_tags", DataTypes.createArrayType(DataTypes.IntegerType), true));

        StructType bidSchema = DataTypes.createStructType(bidFields);

        //        DataFrame bidDf = sqlContext.read().format("com.databricks.spark.csv").schema(bidSchema).option("header", "false").option("delimiter", "\t")
        //                .load("/mnt/data/workspace/laughing-octo-sansa/data/test.txt"); //clk.20130606.txt


        /*
        AdExchange Dict
         */
        List<StructField> adExhangeFields = new ArrayList<StructField>();
        adExhangeFields.add(DataTypes.createStructField("ad_exchange", DataTypes.IntegerType, true));
        adExhangeFields.add(DataTypes.createStructField("ad_exchange_name", DataTypes.StringType, true));

        StructType adExhangeSchema = DataTypes.createStructType(adExhangeFields);

        DataFrame adExchangeDf = sqlContext.read().format("com.databricks.spark.csv").schema(adExhangeSchema).option("header", "false").option("delimiter", "\t")
                .load(Paths.get(dictDir, "ad.exchange.txt").toString());

        /*
        Log Type Dict
         */
        List<StructField> logTypeFields = new ArrayList<StructField>();
        logTypeFields.add(DataTypes.createStructField("log_type_id", DataTypes.IntegerType, true));
        logTypeFields.add(DataTypes.createStructField("log_type_name", DataTypes.StringType, true));

        StructType logTypeSchema = DataTypes.createStructType(logTypeFields);

        DataFrame logTypeDf = sqlContext.read().format("com.databricks.spark.csv").schema(logTypeSchema).option("header", "false").option("delimiter", "\t")
                .load(Paths.get(dictDir, "log.type.txt").toString());

        /*
        City Dict
         */
        List<StructField> cityFields = new ArrayList<StructField>();
        cityFields.add(DataTypes.createStructField("city_id", DataTypes.IntegerType, true));
        cityFields.add(DataTypes.createStructField("city_name", DataTypes.StringType, true));
        cityFields.add(DataTypes.createStructField("state id", DataTypes.IntegerType, true));
        cityFields.add(DataTypes.createStructField("population", DataTypes.StringType, true));
        cityFields.add(DataTypes.createStructField("area", DataTypes.FloatType, true));
        cityFields.add(DataTypes.createStructField("density", DataTypes.FloatType, true));
        cityFields.add(DataTypes.createStructField("latitude", DataTypes.FloatType, true));
        cityFields.add(DataTypes.createStructField("longitude", DataTypes.FloatType, true));

        StructType citySchema = DataTypes.createStructType(cityFields);


        DataFrame cityDf = sqlContext.read().format("com.databricks.spark.csv").schema(citySchema).option("header", "false").option("delimiter", "\t")
                .load(Paths.get(dictDir, "city.us.txt").toString());


         /*
        Region Dict
         */
        //        List<StructField> regionFields = new ArrayList<StructField>();
        //        regionFields.add(DataTypes.createStructField("region", DataTypes.IntegerType, true));
        //        regionFields.add(DataTypes.createStructField("region_name", DataTypes.StringType, true));
        //
        //        StructType regionSchema = DataTypes.createStructType(regionFields);
        //
        //
        //        DataFrame regionDf = sqlContext.read().format("com.databricks.spark.csv").schema(regionSchema).option("header", "false").option("delimiter", "\t")
        //                .load("/mnt/data/workspace/laughing-octo-sansa/data/region.en.txt");

        /*
        US State Dict
         */
        List<StructField> stateFields = new ArrayList<StructField>();
        stateFields.add(DataTypes.createStructField("state_id", DataTypes.IntegerType, true));
        stateFields.add(DataTypes.createStructField("state_name", DataTypes.StringType, true));
        stateFields.add(DataTypes.createStructField("state_population", DataTypes.LongType, true));
        stateFields.add(DataTypes.createStructField("state_gsp", DataTypes.LongType, true));

        StructType stateSchema = DataTypes.createStructType(stateFields);


        DataFrame stateDf = sqlContext.read().format("com.databricks.spark.csv").schema(stateSchema).option("header", "false").option("delimiter", "\t")
                .load(Paths.get(dictDir, "states.us.txt").toString());

        Files.walk(Paths.get(filePath)).forEach(fileName -> {
            if (Files.isRegularFile(fileName)) {
                System.out.println(fileName);

                DataFrame bidDf = sqlContext.read().format("com.databricks.spark.csv").schema(bidSchema).option("header", "false").option("delimiter", "\t")
                        .load(fileName.toString());

                // Dynamic mapping + ES predefined (_timestamp, geo_point, ...)
                DataFrame parsedBidDf = bidDf
                        // Adx
                        .join(adExchangeDf, bidDf.col("ad_exchange").equalTo(adExchangeDf.col("ad_exchange")), "left")
                        .join(logTypeDf, bidDf.col("log_type").equalTo(logTypeDf.col("log_type_id")), "left")
                        .join(cityDf, bidDf.col("city").equalTo(cityDf.col("city_id")), "right")    // src data is messed a bit, so geo_point results in null -> 'right' join is workaround
//                        .join(stateDf, bidDf.col("region").equalTo(stateDf.col("state_id")), "left")

                        .withColumn("user_tags_array", callUDF(new ParseUserTagsArray(), DataTypes.createArrayType(DataTypes.StringType), bidDf.col("user_tags")))
                        .withColumn("coordinates", callUDF(new ParseCoordinates(), DataTypes.createArrayType(DataTypes.FloatType), cityDf.col("latitude"), cityDf.col("longitude")))
                        // User Info
                        .withColumn("user_agent_browser", callUDF(new ParseUserAgentString("browser"), DataTypes.StringType, bidDf.col("user_agent")))
                        .withColumn("user_agent_browserVersion", callUDF(new ParseUserAgentString("browserVersion"), DataTypes.StringType, bidDf.col("user_agent")))
                        .withColumn("user_agent_id", callUDF(new ParseUserAgentString("id"), DataTypes.StringType, bidDf.col("user_agent")))
                        .withColumn("user_agent_OS", callUDF(new ParseUserAgentString("operatingSystem"), DataTypes.StringType, bidDf.col("user_agent")))

                        .drop(bidDf.col("ad_exchange"))
                        .drop(adExchangeDf.col("ad_exchange"))
                        .drop(bidDf.col("log_type"))
                        .drop(logTypeDf.col("log_type_id"))
                        .drop(bidDf.col("city"))
                        .drop(cityDf.col("city_id"))
//                        .drop(bidDf.col("region"))
//                        .drop(stateDf.col("state_id"))
                        .drop(bidDf.col("user_tags"))
                        ;

//                parsedBidDf.show();
//                System.out.println(parsedBidDf.toJSON().toJavaRDD().take(10));
                JavaEsSpark.saveJsonToEs(parsedBidDf.toJSON().toJavaRDD(), esIndex);
            }
        });


        //        bidDf.printSchema();
        //        bidDf.show();
        //
        //        adExchangeDf.printSchema();
        //        adExchangeDf.show();
        //
        //        cityDf.printSchema();
        //        cityDf.show();
        //
        //        regionDf.printSchema();
        //        regionDf.show();

        //        parsedBidDf.toJSON().saveAsTextFile("/mnt/data/workspace/laughing-octo-sansa/data/out2.json");
        //        parsedBidDf.write().json("/mnt/data/workspace/laughing-octo-sansa/data/out1.json");


        //        df.join(dfAdExchange, df.col("C2").equalTo(dfAdExchange.col("C0")), "left").select(df.col("C2"), dfAdExchange.col("C1")).show();
        //        bidDf.join(adExchangeDf, bidDf.col("C2").equalTo(adExchangeDf.col("C0")), "left")
        //                .select(
        //                        bidDf.col("C0"),
        //                        bidDf.col("C1"),
        //                        adExchangeDf.col("C1"),
        //                        bidDf.col("C2"),
        //                        bidDf.col("C23")
        //                )
        //                .show();


        //        df.write().mode("append").json("/mnt/data/workspace/laughing-octo-sansa/data/out.json");
        //        System.out.println("!!!!!!!!!!!!!!");
        //        for (String s : dfAdExchange.columns()) {
        //            System.out.println("!!!!!!!!!!! s = " + s);
        //        }


        //        df.join(dfAdExchange, df.col())

        //        df.withColumn("AdExchange", callUDF(new ParseUserAgentString("browser"), DataTypes.StringType, df.col("user_agent")))

        //        JavaRDD<String> distData = jsc.parallelize(data);
        //        JavaRDD<String> lines = distData.map(line -> String.join("\t",
        //                line,
        //                IpinyouMapper.getAdExchange(Integer.parseInt(line.split("\t")[2]))
        //                )
        //        );
        //        System.out.println(lines.take(10));
    }
}




