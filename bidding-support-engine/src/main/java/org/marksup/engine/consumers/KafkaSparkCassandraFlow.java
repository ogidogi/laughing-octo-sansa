package org.marksup.engine.consumers;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import kafka.serializer.StringDecoder;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.marksup.engine.beans.KafkaRowWithUUID;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.UUID;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;
import static com.datastax.spark.connector.japi.CassandraStreamingJavaUtil.javaFunctions;

public class KafkaSparkCassandraFlow {

    private static final Logger log = Logger.getLogger(KafkaSparkCassandraFlow.class);

    public static void main(String[] args) throws ConfigurationException {
        KafkaSparkCassandraFlow workflow = new KafkaSparkCassandraFlow();
        log.setLevel(Level.DEBUG);

        CompositeConfiguration conf = new CompositeConfiguration();
        conf.addConfiguration(new PropertiesConfiguration("kafka.properties"));
        conf.addConfiguration(new PropertiesConfiguration("spark.properties"));
        conf.addConfiguration(new PropertiesConfiguration("cassandra.properties"));

        try {
            workflow.run(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void run(CompositeConfiguration conf) {
        // Kafka props
        String kafkaBrokers = conf.getString("metadata.broker.list");
        String topics = conf.getString("consumer.topic");
        String fromOffset = conf.getString("auto.offset.reset");

        // Spark props
        String sparkMaster = conf.getString("spark.master");
        long sparkStreamDuration = conf.getLong("stream.duration");

        // Cassandra props
        String cassandraKeyspace = "test";
        String cassandraTable = "kafka_logstream";
        String cassandraDbNode = conf.getString("cassandra.database.node");

        SparkConf sparkConf = new SparkConf().setAppName("Kafka Spark Cassandra Flow with Java API").setMaster(sparkMaster)
                .set("spark.cassandra.connection.host", cassandraDbNode);

        createDdl(sparkConf);

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(sparkStreamDuration));

        HashSet<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        HashMap<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", kafkaBrokers);
        kafkaParams.put("auto.offset.reset", fromOffset);

        // Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class,
                StringDecoder.class, kafkaParams, topicsSet);

        // Get the lines
        // JavaPairDStream<UUID, String> lines = messages.mapToPair(tuple2 -> new Tuple2<UUID, String>(UUID.randomUUID(), tuple2._2()));
        // JavaDStream<String> lines = messages.map(tuple2 -> UUID.randomUUID() + "\t" + tuple2._2());
        JavaDStream<KafkaRowWithUUID> lines = messages.map(tuple2 -> new KafkaRowWithUUID(UUID.randomUUID(), tuple2._2()));
        lines.print();

        javaFunctions(lines).writerBuilder(cassandraKeyspace, cassandraTable, mapToRow(KafkaRowWithUUID.class)).saveToCassandra();

        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }

    private void createDdl(SparkConf conf) {
        CassandraConnector connector = CassandraConnector.apply(conf);

        try (Session session = connector.openSession()) {
            session.execute("DROP KEYSPACE IF EXISTS test");
            session.execute("CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            session.execute("CREATE TABLE test.kafka_logstream (key UUID PRIMARY KEY, value TEXT)");
        }
    }
}
