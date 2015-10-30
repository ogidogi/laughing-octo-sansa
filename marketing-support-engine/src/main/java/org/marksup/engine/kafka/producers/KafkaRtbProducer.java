package org.marksup.engine.kafka.producers;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;

public class KafkaRtbProducer {
    private static final Logger log = Logger.getLogger(KafkaRtbProducer.class);

    public static void main(String[] args) throws ConfigurationException, InterruptedException {
        log.setLevel(Level.DEBUG);
        Random rnd = new Random();

        CompositeConfiguration conf = new CompositeConfiguration();
        conf.addConfiguration(new PropertiesConfiguration("kafka.properties"));

        Properties props = new Properties();
        props.put("metadata.broker.list", "quickstart.cloudera:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        // props.put("partitioner.class", "example.producer.SimplePartitioner");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);

        try (BufferedReader br = new BufferedReader(new FileReader("/media/sf_Download/data/mors/new/site-click.20130606-aa.txt"))) {

            String currentLine;
            int numLines = rnd.nextInt(10) + 1;

            log.info(String.format("Read %d lines...", numLines));
            while ((currentLine = br.readLine()) != null) {
                if (numLines > 0) {
                    KeyedMessage<String, String> data = new KeyedMessage<String, String>("test_topic", currentLine);
                    producer.send(data);
                    numLines--;
                } else {
                    KeyedMessage<String, String> data = new KeyedMessage<String, String>("test_topic", currentLine);
                    producer.send(data);
                    numLines = rnd.nextInt(10) + 1;
                    Thread.sleep(3000);
                    log.info(String.format("Read %d lines...", numLines));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            producer.close();
        }

        log.info("EOF");
        producer.close();
    }
}
