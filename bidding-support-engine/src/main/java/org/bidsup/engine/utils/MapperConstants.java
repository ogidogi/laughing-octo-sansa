package org.bidsup.engine.utils;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.bidsup.engine.utils.MapperConstants.SchemaFields.*;

public class MapperConstants {

    public static enum SchemaFields {

        BID_ID("bid_id", DataTypes.StringType, true),
        TIMESTAMP("timestamp", DataTypes.StringType, true),
//        LOG_TYPE("log_type", DataTypes.IntegerType, true),
        IPINYOU_ID("ipinyou_id", DataTypes.StringType, true),
        USER_AGENT("user_agent", DataTypes.StringType, true),
        IP("ip", DataTypes.StringType, true),
        REGION("region", DataTypes.IntegerType, true),
//        CITY("city", DataTypes.IntegerType, true),
//        AD_EXCHANGE("ad_exchange", DataTypes.IntegerType, true),
        DOMAIN("domain", DataTypes.StringType, true),
        URL("url", DataTypes.StringType, true),
        ANONYMOUS_URL_ID("anonymous_url_id", DataTypes.StringType, true),
        AD_SLOT_ID("ad_slot_id", DataTypes.StringType, true),
        AD_SLOT_WIDTH("ad_slot_width", DataTypes.IntegerType, true),
        AD_SLOT_HEIGHT("ad_slot_height", DataTypes.IntegerType, true),
        AD_SLOT_VISIBILITY("ad_slot_visibility", DataTypes.StringType, true),
        AD_SLOT_FORMAT("ad_slot_format", DataTypes.StringType, true),
        AD_SLOT_FLOOR_PRICE("ad_slot_floor_price", DataTypes.LongType, true),
        CREATIVE_ID("creative_id", DataTypes.StringType, true),
        BIDDING_PRICE("bidding_price", DataTypes.FloatType, true),
        PAYING_PRICE("paying_price", DataTypes.FloatType, true),
        KEY_PAGE_URL("key_page_url", DataTypes.StringType, true),
        ADVERTISER_ID("advertiser_id", DataTypes.LongType, true),
        USER_TAGS("user_tags", DataTypes.StringType, true),

        AD_EXCH_ID("ad_exchange", DataTypes.IntegerType, true),
        AD_EXCH_NAME("ad_exchange_name", DataTypes.StringType, true),
        AD_EXCH_DESC("ad_exchange_desc", DataTypes.StringType, true),

        LOG_TYPE_ID("log_type_id", DataTypes.IntegerType, true),
        LOG_TYPE_NAME("log_type_name", DataTypes.StringType, true),

        CITY_ID("city_id", DataTypes.IntegerType, true),
        CITY_NAME("city_name", DataTypes.StringType, true),
//        STATE_ID("state_id", DataTypes.IntegerType, true),
        CITY_POPULATION("population", DataTypes.StringType, true),
        CITY_AREA("area", DataTypes.FloatType, true),
        CITY_DENSITY("density", DataTypes.FloatType, true),
        CITY_LATITUDE("latitude", DataTypes.FloatType, true),
        CITY_LONGITUDE("longitude", DataTypes.FloatType, true),

        STATE_ID("state_id", DataTypes.IntegerType, true),
        STATE_NAME("state_name", DataTypes.StringType, true),
        STATE_POPULATION("state_population", DataTypes.LongType, true),
        STATE_GSP("state_gsp", DataTypes.LongType, true),

        KEYWORD_ID("keyword_id", DataTypes.IntegerType, true),
        KEYWORD_VALUE("keyword_value", DataTypes.StringType, true),
        KEYWORD_STATUS("keyword_status", DataTypes.StringType, true),
        KEYWORD_PRICING_TYPE("keyword_pricing_type", DataTypes.StringType, true),
        KEYWORD_MATCH_TYPE("keyword_match_type", DataTypes.StringType, true),

        ;

        private final StructField structField;

        SchemaFields(String fieldName, DataType fieldType, Boolean isNullable) {
            structField = DataTypes.createStructField(fieldName, fieldType, isNullable);
        }

        public StructField getStructField() {
            return structField;
        }
    }


    public static enum MappingSchemas {
        BID_SCHEMA(BID_ID, TIMESTAMP, LOG_TYPE_ID, IPINYOU_ID, USER_AGENT, IP, REGION, CITY_ID, AD_EXCH_ID, DOMAIN, URL,
                ANONYMOUS_URL_ID, AD_SLOT_ID, AD_SLOT_WIDTH, AD_SLOT_HEIGHT, AD_SLOT_VISIBILITY, AD_SLOT_FORMAT,
                AD_SLOT_FLOOR_PRICE, CREATIVE_ID, BIDDING_PRICE, PAYING_PRICE, KEY_PAGE_URL, ADVERTISER_ID, USER_TAGS),
        AD_EXCH_SCHEMA(AD_EXCH_ID, AD_EXCH_NAME, AD_EXCH_DESC),
        LOG_TYPE_SCHEMA(LOG_TYPE_ID, LOG_TYPE_NAME),
        CITY_SCHEMA(CITY_ID, CITY_NAME, STATE_ID, CITY_POPULATION, CITY_AREA, CITY_DENSITY, CITY_LATITUDE, CITY_LONGITUDE),
        STATE_SCHEMA(STATE_ID, STATE_NAME, STATE_POPULATION, STATE_GSP),
        KEYWORD_SCHEMA(KEYWORD_ID, KEYWORD_VALUE, KEYWORD_STATUS, KEYWORD_PRICING_TYPE, KEYWORD_MATCH_TYPE),

        ;

        private final StructType schema;

        MappingSchemas(SchemaFields... fields) {
            schema = DataTypes.createStructType(Arrays.stream(fields).map(SchemaFields::getStructField).collect(Collectors.toList()));
        }

        public StructType getSchema() {
            return schema;
        }
    }
}