package org.bidsup.engine.crawlers;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;


public class IpinyouParse {
    public static void main(String[] args) throws IOException {
        String es_index = "rtb/log_item";

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
                .load("/mnt/data/workspace/laughing-octo-sansa/data/AdExchange.txt");

        /*
        Log Type Dict
         */
        List<StructField> logTypeFields = new ArrayList<StructField>();
        logTypeFields.add(DataTypes.createStructField("log_type_id", DataTypes.IntegerType, true));
        logTypeFields.add(DataTypes.createStructField("log_type_name", DataTypes.StringType, true));

        StructType logTypeSchema = DataTypes.createStructType(logTypeFields);

        DataFrame logTypeDf = sqlContext.read().format("com.databricks.spark.csv").schema(logTypeSchema).option("header", "false").option("delimiter", "\t")
                .load("/mnt/data/workspace/laughing-octo-sansa/data/LogType.txt");

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
                .load("/mnt/data/workspace/laughing-octo-sansa/data/city.us.txt");


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
                .load("/mnt/data/workspace/laughing-octo-sansa/data/states.us.txt");

        Files.walk(Paths.get("/media/sf_Download/ipinyou/")).forEach(filePath -> {
            if (Files.isRegularFile(filePath)) {
                System.out.println(filePath);

                DataFrame bidDf = sqlContext.read().format("com.databricks.spark.csv").schema(bidSchema).option("header", "false").option("delimiter", "\t")
                        .load(filePath.toString());

                // Dynamic mapping + ES predefined (_timestamp, geo_point)
                DataFrame parsedBidDf = bidDf
                        .join(adExchangeDf, bidDf.col("ad_exchange").equalTo(adExchangeDf.col("ad_exchange")), "left")
                        .join(logTypeDf, bidDf.col("log_type").equalTo(logTypeDf.col("log_type_id")), "left")
                        .join(cityDf, bidDf.col("city").equalTo(cityDf.col("city_id")), "right")    // src data is messed a bit, so geo_point results in null -> 'right' join is workaround
//                        .join(stateDf, bidDf.col("region").equalTo(stateDf.col("state_id")), "left")
                        .withColumn("parser_user_tags", callUDF(new ParseUserTagsArrayTest(), DataTypes.createArrayType(DataTypes.StringType), bidDf.col("user_tags")))
                        .withColumn("coordinates", callUDF(new ParseCoordinatesTest(), DataTypes.createArrayType(DataTypes.FloatType), cityDf.col("latitude"), cityDf.col("longitude")))
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

                //        parsedBidDf.show();
                //        System.out.println(parsedBidDf.toJSON().toJavaRDD().take(10));
                JavaEsSpark.saveJsonToEs(parsedBidDf.toJSON().toJavaRDD(), es_index);
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

//        df.withColumn("AdExchange", callUDF(new ParseUserAgentString("browser"), DataTypes.StringType, df.col("C4")))

//        JavaRDD<String> distData = jsc.parallelize(data);
//        JavaRDD<String> lines = distData.map(line -> String.join("\t",
//                line,
//                IpinyouMapper.getAdExchange(Integer.parseInt(line.split("\t")[2]))
//                )
//        );
//        System.out.println(lines.take(10));
    }
}

class ParseUserTagsArray extends AbstractFunction1<String, List<String>> implements Serializable {


    private static final long serialVersionUID = -1484997808699658439L;

    public ParseUserTagsArray() {
        super();
    }

    @Override
    public List<String> apply(String value) {
//        return Arrays.stream(value.split(",")).map(x -> Integer.parseInt(x)).collect(Collectors.toList());
        if (value == null) {
            return Arrays.asList("0");
        }
        return Arrays.stream(value.split(",")).collect(Collectors.toList());
    }
}

class ParseCoordinates extends AbstractFunction2<Float, Float, List<Float>> implements Serializable {


    private static final long serialVersionUID = -1494997808699658439L;

    public ParseCoordinates() {
        super();
    }

    @Override
    public List<Float> apply(Float lat, Float lon) {
        if (lat == null || lon == null) {
            return Arrays.asList((float) 0, (float) 0);
        }
        // Format in [lon, lat], note, the order of lon/lat here in order to conform with GeoJSON.
        // https://www.elastic.co/guide/en/elasticsearch/reference/1.3/mapping-geo-point-type.html#_lat_lon_as_array_5
        return Arrays.asList(lon, lat);
    }
}
