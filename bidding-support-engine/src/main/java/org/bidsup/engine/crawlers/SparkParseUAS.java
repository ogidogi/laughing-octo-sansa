package org.bidsup.engine.crawlers;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;

import eu.bitwalker.useragentutils.UserAgent;
import scala.runtime.AbstractFunction1;

import static org.apache.spark.sql.functions.*;

public class SparkParseUAS {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("SparkParseUAS").setMaster("local[2]").set("spark.serializer",
                "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(jsc);

        DataFrame df = sqlContext.read().format("com.databricks.spark.csv").option("header", "false").option("delimiter", "\t")
                .load("/mnt/data/workspace/laughing-octo-sansa/data/clk.20130606.txt");
        df.withColumn("browser", callUDF(new ParseUserAgentString("browser"), DataTypes.StringType, df.col("C4")))
                .withColumn("browserVersion", callUDF(new ParseUserAgentString("browserVersion"), DataTypes.StringType, df.col("C4")))
                .withColumn("id", callUDF(new ParseUserAgentString("id"), DataTypes.StringType, df.col("C4")))
                .withColumn("operatingSystem", callUDF(new ParseUserAgentString("operatingSystem"), DataTypes.StringType, df.col("C4")))
                .write().format("com.databricks.spark.csv").option("header", "false").option("delimiter", "\t")
                .save("/mnt/data/workspace/laughing-octo-sansa/data/clk.20130606_parsed.txt");
    }
}

class ParseUserAgentString extends AbstractFunction1<String, String>implements Serializable {

    private static final long serialVersionUID = -1474997808699658439L;

    private final String userAgentComponent;

    public ParseUserAgentString(String userAgentComponent) {
        this.userAgentComponent = userAgentComponent;
    }

    @Override
    public String apply(String value) {
        final UserAgent userAgent = UserAgent.parseUserAgentString(value);
        switch (userAgentComponent) {
        case "browser":
            return String.valueOf(userAgent.getBrowser());
        case "browserVersion":
            return String.valueOf(userAgent.getBrowserVersion());
        case "id":
            return String.valueOf(userAgent.getId());
        case "operatingSystem":
            return String.valueOf(userAgent.getOperatingSystem());
        default:
            return null;
        }
    }

}
