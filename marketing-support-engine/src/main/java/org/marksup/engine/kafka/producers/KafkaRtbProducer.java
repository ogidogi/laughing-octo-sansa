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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Random;

import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;

public class KafkaRtbProducer {
    private static final Logger log = Logger.getLogger(KafkaRtbProducer.class);
    private static final int sleepMsInterval = 4000;
    private static Path dirPath = Paths.get("/media/sf_Download/data/mors/new");


    public static void main(String[] args) throws ConfigurationException, InterruptedException, IOException {
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
//        Producer<String, String> producer = new Producer<String, String>(config);

        Files.walk(dirPath).forEach(file -> {
            if (Files.isRegularFile(file)) {
                String fileName = file.getFileName().toString();
                log.info(String.format("[%s] Processing %s", currentThread().getName(), fileName));

                Thread fileThread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        Producer<String, String> producer = new Producer<String, String>(config);
                        try (BufferedReader br = new BufferedReader(new FileReader(file.toFile()))) {
                            String currentLine;
                            int numLines = rnd.nextInt(10) + 1;

                            log.info(String.format("[%s]:[%s] Read %d lines...", currentThread().getName(), fileName, numLines));
                            while ((currentLine = br.readLine()) != null) {
                                if (numLines > 0) {
                                    KeyedMessage<String, String> data = new KeyedMessage<String, String>("test_topic", currentLine);
                                    producer.send(data);
                                    numLines--;
                                } else {
                                    KeyedMessage<String, String> data = new KeyedMessage<String, String>("test_topic", currentLine);
                                    producer.send(data);
                                    numLines = rnd.nextInt(10) + 1;
                                    sleep(sleepMsInterval);
                                    log.info(String.format("[%s]:[%s] Read %d lines...", currentThread().getName(), fileName, numLines));
                                }
                            }
                        } catch (IOException | InterruptedException e) {
                            e.printStackTrace();
                            producer.close();
                        }

                        log.info(String.format("[%s]:[%s] end-of-file", currentThread().getName(), fileName));
                        producer.close();
                    }
                });
                fileThread.start();
            }
        });
    }
}