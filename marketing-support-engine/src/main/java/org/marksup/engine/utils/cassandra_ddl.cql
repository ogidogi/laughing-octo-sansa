
DROP KEYSPACE IF EXISTS test;
CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

use test;
DROP TABLE test.predict_kafka_raw;
CREATE TABLE test.predict_kafka_raw (
    bid_id                          TEXT
    ,timestamp                      TEXT
    ,ipinyou_id                     TEXT
    ,user_agent TEXT
    ,ip TEXT
    ,domain TEXT
    ,url    TEXT
    ,anonymous_url_id    TEXT
    ,ad_slot_id TEXT
    ,ad_slot_width  TEXT
    ,ad_slot_height TEXT
    ,ad_slot_visibility             TEXT
    ,ad_slot_format             TEXT
    ,ad_slot_floor_price                TEXT
    ,creative_id                TEXT
    ,bidding_price              TEXT
    ,advertiser_id              TEXT
    ,paying_price               TEXT
    ,ad_exchange_name               TEXT
    ,ad_exchange_desc               TEXT
    ,log_type_name              TEXT
    ,city_name              TEXT
    ,population             TEXT
    , area              TEXT
    ,density                TEXT
    ,coordinates                TEXT
    ,state_name             TEXT
    ,state_population               TEXT
    ,state_gsp              TEXT
    ,keyword_name               TEXT
    ,ua_browser             TEXT
    ,ua_browser_group               TEXT
    ,ua_browser_manufacturer            TEXT
    ,ua_browser_rendering_engine            TEXT
    ,ua_browser_version          TEXT
    ,ua_browser_version_minor            TEXT
    ,ua_browser_version_major            TEXT
    ,ua_id          TEXT
    ,ua_os          TEXT
    ,ua_os_name         TEXT
    ,ua_os_device           TEXT
    ,ua_os_group            TEXT
    ,ua_os_manufacturer         TEXT
    ,PRIMARY KEY (ipinyou_id, timestamp)
)
;

