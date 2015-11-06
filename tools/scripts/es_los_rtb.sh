// -----------------------
// Common
// -----------------------

curl -XDELETE 'http://localhost:9200/rtb?pretty'
curl -XDELETE 'http://localhost:9200/rtb/_all?pretty'
// curl -XPOST 'http://localhost:9200/rtb?pretty'

//get mapping
curl -XGET 'http://localhost:9200/rtb/_mapping/log_item'


// Get all Docs in Index
curl -XGET 'localhost:9200/rtb/_search?search_type=scan&scroll=10m&size=50' -d '
{
    "query" : {
        "match_all" : {}
    }
}'

http://localhost:9200/rtb/_search?pretty=true&q=*:*


// Drop all docs by type
curl -XDELETE 'http://localhost:9200/rtb/log_item/_query' -d '
    {
      "query": {
        "bool": {
          "must": [
            {
              "match_all": {}
            }
          ]
        }
      }
  }'

// Drop doc by id
curl -XDELETE 'http://localhost:9200/habrahabr/users/1'


// -----------------------
// KEYWORDS
// -----------------------
curl -XPOST 'http://localhost:9200/_template/kw_template?pretty' -d '
{
    "template" : "keyword",
    "settings" : {
        "number_of_shards" : 1
    },
    "mappings" : {
        "log": {
           "_timestamp": {
                   "enabled": true
           },
           "dynamic_templates": [{
               "template_user_agent": {
                   "match": "ua*",
                   "match_mapping_type": "string",
                   "mapping": {
                       "type": "string",
                       "index": "not_analyzed"
                   }
               }
           },
           {
               "template_adx_exhange": {
                   "match": "ad_exchange_*",
                   "match_mapping_type": "string",
                   "mapping": {
                       "type": "string",
                       "index": "not_analyzed"
                   }
               }
           }],
           "properties": {
               "coordinates": {
                   "type": "geo_point"
               },
               "timestamp": {
                   "type": "date",
                   "format": "yyyyMMddHHmmssSSS"
               },
               "user_agent": {
                   "type": "string",
                   "index": "not_analyzed"
               },
               "log_type_name": {
                    "type": "string",
                    "index": "not_analyzed"
                },
                "city_name": {
                    "type": "string",
                    "index": "not_analyzed"
                },
                "state_name": {
                    "type": "string",
                    "index": "not_analyzed"
                },
                "keyword_name": {
                    "type": "string",
                    "analyzer": "english"
                },
                "ad_slot_size": {
                   "type": "string",
                   "index": "not_analyzed"
               }
           }
       }
    }
}
'



curl -XDELETE 'http://localhost:9200/keyword/log/_query' -d '
    {
      "query": {
        "bool": {
          "must": [
            {
              "match_all": {}
            }
          ]
        }
      }
  }'


// -----------------------
// PREDICTION
// -----------------------
curl -XPOST 'http://localhost:9200/_template/test_stream_template?pretty' -d '
{
    "template" : "test_stream",
    "settings" : {
        "number_of_shards" : 1
    },
    "mappings" : {
        "log": {
            "_timestamp": {
                   "enabled": true
            },
           "dynamic_templates": [{
               "template_user_agent": {
                   "match": "ua*",
                   "match_mapping_type": "string",
                   "mapping": {
                       "type": "string",
                       "index": "not_analyzed"
                   }
               }
           },
           {
               "template_adx_exhange": {
                   "match": "ad_exchange_*",
                   "match_mapping_type": "string",
                   "mapping": {
                       "type": "string",
                       "index": "not_analyzed"
                   }
               }
           }],
           "properties": {
               "coordinates": {
                   "type": "geo_point"
               },
               "timestamp": {
                   "type": "date",
                   "format": "yyyyMMddHHmmssSSS"
               },
               "user_agent": {
                   "type": "string",
                   "index": "not_analyzed"
               },
               "log_type_name": {
                    "type": "string",
                    "index": "not_analyzed"
                },
                "city_name": {
                    "type": "string",
                    "index": "not_analyzed"
                },
                "state_name": {
                    "type": "string",
                    "index": "not_analyzed"
                },
                "keyword_name": {
                    "type": "string",
                    "analyzer": "english"
                },
                "predict": {
                   "type": "string",
                   "index": "not_analyzed"
               },
               "false_prob": {
                   "type": "double",
                   "null_value" : 0.0
               },
               "true_prob": {
                   "type": "double",
                   "null_value" : 0.0
               }
           }
       }
    }
}
'



curl -XDELETE 'http://localhost:9200/stream/log/_query' -d '
    {
      "query": {
        "bool": {
          "must": [
            {
              "match_all": {}
            }
          ]
        }
      }
  }'