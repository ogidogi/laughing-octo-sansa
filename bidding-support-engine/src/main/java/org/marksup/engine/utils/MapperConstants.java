package org.marksup.engine.utils;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.marksup.engine.utils.MapperConstants.SchemaFields.*;

import java.util.Arrays;
import java.util.stream.Collectors;

public class MapperConstants {
    public enum SchemaFields {
        BID_ID("bid_id", DataTypes.StringType, true),
        TIMESTAMP("timestamp", DataTypes.StringType, true),
        IPINYOU_ID("ipinyou_id", DataTypes.StringType, true),
        USER_AGENT("user_agent", DataTypes.StringType, true),
        IP("ip", DataTypes.StringType, true),
        REGION("region", DataTypes.IntegerType, true),
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

        USER_TAG_ID("user_tags", DataTypes.LongType, true),

        AD_EXCH_ID("ad_exchange_id", DataTypes.IntegerType, true),
        AD_EXCH_NAME("ad_exchange_name", DataTypes.StringType, true),
        AD_EXCH_DESC("ad_exchange_desc", DataTypes.StringType, true),

        LOG_TYPE_ID("log_type_id", DataTypes.IntegerType, true),
        LOG_TYPE_NAME("log_type_name", DataTypes.StringType, true),

        CITY_ID("city_id", DataTypes.IntegerType, true),
        CITY_NAME("city_name", DataTypes.StringType, true),
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
        KEYWORD_NAME("keyword_name", DataTypes.StringType, true),

        USER_TAGS_ARRAY("user_tags_array", DataTypes.createArrayType(DataTypes.StringType), true),
        COORDINATES("coordinates", DataTypes.createArrayType(DataTypes.FloatType), false),

        UA_BROWSER("ua_browser", DataTypes.StringType, true),
        UA_BROWSER_TYPE("ua_browser_type", DataTypes.StringType, true),
        UA_BROWSER_GROUP("ua_browser_group", DataTypes.StringType, true),
        UA_BROWSER_MANUFACTURER("ua_browser_manufacturer", DataTypes.StringType, true),
        UA_BROWSER_RENDERING_ENGINE("ua_browser_rendering_engine", DataTypes.StringType, true),

        UA_BROWSERVERSION("ua_browserVersion", DataTypes.StringType, true),
        UA_BROWSERVERSION_MINOR("ua_browserVersion_minor", DataTypes.StringType, true),
        UA_BROWSERVERSION_MAJOR("ua_browserVersion_major", DataTypes.StringType, true),

        UA_ID("ua_id", DataTypes.StringType, true),

        UA_OS("ua_os", DataTypes.StringType, true),
        UA_OS_NAME("ua_os_name", DataTypes.StringType, true),
        UA_OS_DEVICE("ua_os_device", DataTypes.StringType, true),
        UA_OS_GROUP("ua_os_group", DataTypes.StringType, true),
        UA_OS_MANUFACTURER("ua_os_manufacturer", DataTypes.StringType, true),

        SITE_PAGE_ID("site_page_id", DataTypes.LongType, true),
        SITE_PAGE_URL("site_page_url", DataTypes.StringType, true),
        SITE_PAGE_TAG("site_page_tag", DataTypes.StringType, true),

        USER_PROFILE_TAG_ID("user_profile_tag_id", DataTypes.LongType, true),
        USER_PROFILE_TAG_VALUE("user_profile_tag_value", DataTypes.StringType, true),
        USER_PROFILE_TAG_PRICE_TYPE("user_profile_tag_price_type", DataTypes.StringType, true),
        USER_PROFILE_TAG_MATCH_TYPE("user_profile_tag_match_type", DataTypes.StringType, true),
        USER_PROFILE_TAG_DEST_URL("user_profile_tag_dest_url", DataTypes.StringType, true),
        ;

        private final StructField structField;

        SchemaFields(String fieldName, DataType fieldType, Boolean isNullable) {
            structField = DataTypes.createStructField(fieldName, fieldType, isNullable);
        }

        public StructField getStructField() {
            return structField;
        }
        public String getName() {
            return this.getStructField().name();
        }
    }


    public enum MappingSchemas {
        BID_SCHEMA(BID_ID, TIMESTAMP, LOG_TYPE_ID, IPINYOU_ID, USER_AGENT, IP, REGION, CITY_ID, AD_EXCH_ID, DOMAIN, URL,
                ANONYMOUS_URL_ID, AD_SLOT_ID, AD_SLOT_WIDTH, AD_SLOT_HEIGHT, AD_SLOT_VISIBILITY, AD_SLOT_FORMAT,
                AD_SLOT_FLOOR_PRICE, CREATIVE_ID, BIDDING_PRICE, PAYING_PRICE, KEY_PAGE_URL, ADVERTISER_ID, USER_TAGS),
        BID_LOG_SCHEMA(BID_ID, TIMESTAMP, IPINYOU_ID, USER_AGENT, IP, REGION, CITY_ID, AD_EXCH_ID, DOMAIN, URL,
                ANONYMOUS_URL_ID, AD_SLOT_ID, AD_SLOT_WIDTH, AD_SLOT_HEIGHT, AD_SLOT_VISIBILITY, AD_SLOT_FORMAT,
                AD_SLOT_FLOOR_PRICE, CREATIVE_ID, BIDDING_PRICE, ADVERTISER_ID, USER_TAGS, LOG_TYPE_ID, PAYING_PRICE),
        AD_EXCH_SCHEMA(AD_EXCH_ID, AD_EXCH_NAME, AD_EXCH_DESC),
        LOG_TYPE_SCHEMA(LOG_TYPE_ID, LOG_TYPE_NAME),
        CITY_SCHEMA(CITY_ID, CITY_NAME, STATE_ID, CITY_POPULATION, CITY_AREA, CITY_DENSITY, CITY_LATITUDE, CITY_LONGITUDE),
        STATE_SCHEMA(STATE_ID, STATE_NAME, STATE_POPULATION, STATE_GSP),
        KEYWORD_SCHEMA(KEYWORD_ID, KEYWORD_NAME),
        SITE_PAGES_SCHEMA(SITE_PAGE_ID, SITE_PAGE_URL, SITE_PAGE_TAG),
        USER_PROFILE_TAGS_SCHEMA(USER_PROFILE_TAG_ID, USER_PROFILE_TAG_VALUE, USER_PROFILE_TAG_PRICE_TYPE,
                                                                USER_PROFILE_TAG_MATCH_TYPE, USER_PROFILE_TAG_DEST_URL);

        private final StructType schema;

        MappingSchemas(SchemaFields... fields) {
            schema = DataTypes.createStructType(Arrays.stream(fields).map(SchemaFields::getStructField).collect(Collectors.toList()));
        }

        public StructType getSchema() {
            return schema;
        }
    }
}