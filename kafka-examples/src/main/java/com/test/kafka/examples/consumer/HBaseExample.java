package com.test.kafka.examples.consumer;

import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;


public class HBaseExample {
    private List<String> replicaBrokers;

    public HBaseExample() {
        this.replicaBrokers = new ArrayList<>();
    }

    //TODO Add log4j, javadoc
    public static void main(String[] args) throws ConfigurationException {
        HBaseExample example = new HBaseExample();

        CompositeConfiguration conf = new CompositeConfiguration();
        conf.addConfiguration(new PropertiesConfiguration("kafka.properties"));
        conf.addConfiguration(new PropertiesConfiguration("hbase.properties"));

        try {
            example.run(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void run(Configuration conf) throws Exception {
        // Kafka props
        List<String> kafkaBrokers = conf.getList("consumer.servers").stream()
                .map(i -> (String) i).collect(Collectors.toList());
        String topic = conf.getString("consumer.topic");
        int brokerPort = conf.getInt("consumer.servers.port");
        int partition = conf.getInt("consumer.partition");
        long maxReads = conf.getLong("consumer.maxreads");

        // HBase props
        String colFamily = conf.getString("column.family");
        String colQual = conf.getString("column.qual");

        Connection conn = ConnectionFactory.createConnection();
        TableName tablename = TableName.valueOf(conf.getString("table.name"));
        Table t = conn.getTable(tablename);

        PartitionMetadata metadata = findLeader(kafkaBrokers, brokerPort, topic, partition);

        if (metadata == null) {
            System.out.println("Can't find metadata for Topic and Partition. Exiting");
            return;
        }
        if (metadata.leader() == null) {
            System.out.println("Can't find Leader for Topic and Partition. Exiting");
            return;
        }

        String leadBroker = metadata.leader().host();
        String clientName = "CLIENT_" + topic.toUpperCase() + "_" + partition;

        SimpleConsumer consumer = new SimpleConsumer(leadBroker, brokerPort, 100000, 64 * 1024, clientName);
        long readOffset = getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime(),
                clientName);

        int numErrors = 0;

        while (maxReads > 0) {
            if (consumer == null) {
                consumer = new SimpleConsumer(leadBroker, brokerPort, 100000, 64 * 1024, clientName);
            }

            kafka.api.FetchRequest req = new FetchRequestBuilder().clientId(clientName).addFetch(topic, partition,
                    readOffset, 100000) // Note: this fetchSize of 100000 might need to be increased if large batches
                    // are written to Kafka
                    .build();
            FetchResponse fetchResponse = consumer.fetch(req);

            if (fetchResponse.hasError()) {
                numErrors++;
                // Something went wrong!
                short code = fetchResponse.errorCode(topic, partition);
                System.out.println("Error fetching data from the Broker:" + leadBroker + " Reason: " + code);
                if (numErrors > 5)
                    break;
                if (code == ErrorMapping.OffsetOutOfRangeCode()) {
                    // We asked for an invalid offset. For simple case ask for the last element to reset
                    readOffset = getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime(),
                            clientName);
                    continue;
                }
                consumer.close();
                consumer = null;
                leadBroker = findNewLeader(leadBroker, topic, partition, brokerPort);
                continue;
            }

            numErrors = 0;

            long numRead = 0;

            for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition)) {
                long currentOffset = messageAndOffset.offset();
                if (currentOffset < readOffset) {
                    System.out.println("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
                    continue;
                }
                //TODO Check if nextOffset should be in loop
                readOffset = messageAndOffset.nextOffset();
                ByteBuffer payload = messageAndOffset.message().payload();

                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                System.out.println(String.valueOf(messageAndOffset.offset()) + ": " + new String(bytes, "UTF-8"));

                putRowHBase(t, colFamily, colQual, bytes);

                numRead++;
                maxReads--;
            }

            if (numRead == 0) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }

        t.close();
        conn.close();

        if (consumer != null)
            consumer.close();
    }

    private long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime, String
            clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));

        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                requestInfo,
                kafka.api.OffsetRequest.CurrentVersion(),
                clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            System.out.println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic,
                    partition));
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }

    private String findNewLeader(String oldLeader, String topic, int partition, int brokerPort) throws Exception {
        for (int i = 0; i < 3; i++) {
            boolean goToSleep = false;
            PartitionMetadata metadata = findLeader(this.replicaBrokers, brokerPort, topic, partition);
            if (metadata == null) {
                goToSleep = true;
            } else if (metadata.leader() == null) {
                goToSleep = true;
            } else if (oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                // first time through if the leader hasn't changed give ZooKeeper a second to recover
                // second time, assume the broker did recover before failover, or it was a non-Broker issue
                //
                goToSleep = true;
            } else {
                return metadata.leader().host();
            }
            if (goToSleep) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
        System.out.println("Unable to find new leader after Broker failure. Exiting");
        throw new Exception("Unable to find new leader after Broker failure. Exiting");
    }

    public PartitionMetadata findLeader(List<String> kafkaBrokers, int brokerPort, String topic, int partition) {
        PartitionMetadata returnMetaData = null;
        loop:
        for (String broker : kafkaBrokers) {
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(broker, brokerPort, 100000, 64 * 1024, "LEADER_LOOKUP");
                //TODO Pass list of topcis against single topic
                List<String> topics = Collections.singletonList(topic);

                TopicMetadataRequest request = new TopicMetadataRequest(topics);
                TopicMetadataResponse response = consumer.send(request);

                List<TopicMetadata> topicsMeta = response.topicsMetadata();
                for (TopicMetadata topicMeta : topicsMeta) {
                    for (PartitionMetadata partMeta : topicMeta.partitionsMetadata()) {
                        if (partMeta.partitionId() == partition) {
                            returnMetaData = partMeta;
                            break loop;
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("Error communicating with Broker [" + broker + "] to find Leader for [" + topic +
                        ", " + partition + "] Reason: " + e);
            } finally {
                if (consumer != null)
                    consumer.close();
            }
        }

        // Fetch replica brokers for finding new leader in case of failures
        if (returnMetaData != null) {
            this.replicaBrokers.clear();
            this.replicaBrokers.addAll(
                    returnMetaData
                            .replicas().stream()
                            .map(Broker::host)
                            .collect(Collectors.toList())
            );
        }

        return returnMetaData;
    }

    private void putRowHBase(Table t, String colFamily, String colQual, byte[] value) throws IOException {
        long rowKey = System.currentTimeMillis();
        t.put(new Put(Bytes.toBytes(rowKey)).addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(colQual), value));
    }
}
