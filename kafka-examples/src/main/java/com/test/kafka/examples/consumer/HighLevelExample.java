package com.test.kafka.examples.consumer;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class HighLevelExample {
    private final ConsumerConnector consumer;
    private final String topic;
    private ExecutorService executor;

    public HighLevelExample(Configuration conf) {
        Properties props = new Properties();
        props.put("zookeeper.connect", conf.getString("zookeeper.connect"));
        props.put("group.id", conf.getString("consumer.group"));
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        this.topic = conf.getString("consumer.topic");

        System.out.println("topic = [" + this.topic + "]");
    }

    public static void main(String[] args) throws ConfigurationException {
        //int threads = Integer.parseInt(args[3]);
        int threads = 10;

        HighLevelExample example = new HighLevelExample(new PropertiesConfiguration("kafka.properties"));
        example.run(threads);

        try {
            Thread.sleep(10000);
        } catch (InterruptedException ie) {

        }
        example.shutdown();
    }

    public void shutdown() {
        if (consumer != null)
            consumer.shutdown();
        if (executor != null)
            executor.shutdown();
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            System.out.println("Interrupted during shutdown, exiting uncleanly");
        }
    }

    public void run(int a_numThreads) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(a_numThreads));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        // now launch all the threads
        //
        executor = Executors.newFixedThreadPool(a_numThreads);

        // now create an object to consume the messages
        //
        int threadNumber = 0;
        for (final KafkaStream<byte[], byte[]> stream : streams) {
            executor.submit(new ConsumerTest(stream, threadNumber));
            threadNumber++;
        }
    }


    class ConsumerTest implements Runnable {
        private KafkaStream<byte[], byte[]> m_stream;
        private int m_threadNumber;

        public ConsumerTest(KafkaStream<byte[], byte[]> a_stream, int a_threadNumber) {
            m_threadNumber = a_threadNumber;
            m_stream = a_stream;
        }

        public void run() {
            ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
            while (it.hasNext())
                System.out.println("Thread " + m_threadNumber + ": " + new String(it.next().message()));
            System.out.println("Shutting down Thread: " + m_threadNumber);
        }
    }
}
