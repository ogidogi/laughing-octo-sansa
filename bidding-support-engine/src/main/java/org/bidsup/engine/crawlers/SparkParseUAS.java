package org.bidsup.engine.crawlers;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.bidsup.engine.spark.sql.udf.ParseUserAgentString;

import static org.apache.spark.sql.functions.callUDF;

public class SparkParseUAS {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("SparkParseUAS").setMaster("local[4]").set("spark.serializer",
                "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(jsc);

        DataFrame df = sqlContext.read().format("com.databricks.spark.csv").option("header", "false").option("delimiter", "\t")
<<<<<<< Updated upstream
                .load("/mnt/data/workspace/laughing-octo-sansa/data/clk.20130606.txt");

        df.withColumn("browser", callUDF(new ParseUserAgentString("browser"), DataTypes.StringType, df.col("C4")))
                .withColumn("browserVersion", callUDF(new ParseUserAgentString("browserVersion"), DataTypes.StringType, df.col("C4")))
                .withColumn("id", callUDF(new ParseUserAgentString("id"), DataTypes.StringType, df.col("C4")))
                .withColumn("operatingSystem", callUDF(new ParseUserAgentString("operatingSystem"), DataTypes.StringType, df.col("C4")))
=======
                .load("/mnt/data/workspace/laughing-octo-sansa/data/bid.20130606.txt");
        df.withColumn("browser", callUDF(new ParseUserAgentString("browser"), DataTypes.StringType, df.col("C3")))
                .withColumn("browserVersion", callUDF(new ParseUserAgentString("browserVersion"), DataTypes.StringType, df.col("C3")))
                .withColumn("id", callUDF(new ParseUserAgentString("id"), DataTypes.StringType, df.col("C3")))
                .withColumn("operatingSystem", callUDF(new ParseUserAgentString("operatingSystem"), DataTypes.StringType, df.col("C3")))
>>>>>>> Stashed changes
                .write().format("com.databricks.spark.csv").option("header", "false").option("delimiter", "\t")
                .save("/mnt/data/workspace/laughing-octo-sansa/data/bid.20130606_parsed.txt");
    }
}
