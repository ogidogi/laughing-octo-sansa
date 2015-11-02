package org.marksup.engine.consumers;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.examples.h2o.CraigslistJobTitlesApp;
import org.apache.spark.h2o.H2OContext;
import org.apache.spark.mllib.feature.Word2VecModel;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import hex.Model;
import kafka.serializer.StringDecoder;
import scala.Tuple2;
import water.Key;
import water.api.Handler;
import water.exceptions.H2OIllegalArgumentException;
import water.fvec.H2OFrame;
import water.serial.ObjectTreeBinarySerializer;
import water.util.FileUtils;

public class StreamingUserTypeClassification {
    private static final Logger log = Logger.getLogger(StreamingUserTypeClassification.class);
    private static final String craigslistJobTitles = "/mnt/data/workspace/laughing-octo-sansa/data/craigslistJobTitles.csv";
    private static final String h2oModelFolder = "/mnt/data/workspace/laughing-octo-sansa/data/h2oModel";
//    private static final String h2oModelFolder1 = "/mnt/data/workspace/laughing-octo-sansa/data/deep_learning_model";
    private static final String h2oModelFolder1 = "/mnt/data/workspace/laughing-octo-sansa/data/deep_learning_model2";
//    private static final String word2VecModelFolder = "/mnt/data/workspace/laughing-octo-sansa/data/word2VecModel";

    public static void main(String[] args) throws ConfigurationException {
        StreamingUserTypeClassification workflow = new StreamingUserTypeClassification();
        log.setLevel(Level.DEBUG);

        CompositeConfiguration conf = new CompositeConfiguration();
        conf.addConfiguration(new PropertiesConfiguration("kafka.properties"));
        conf.addConfiguration(new PropertiesConfiguration("spark.properties"));
        conf.addConfiguration(new PropertiesConfiguration("es.properties"));

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
        String sparkSerDe = conf.getString("spark.serializer");
        long sparkStreamDuration = conf.getLong("stream.duration");

        SparkConf sparkConf = new SparkConf().setAppName("Kafka Spark ES Flow with Java API").setMaster(sparkMaster).set("spark.serializer",
                sparkSerDe);

        JavaSparkContext sp = new JavaSparkContext(sparkConf);
        JavaStreamingContext jssc = new JavaStreamingContext(sp, Durations.seconds(sparkStreamDuration));
        SQLContext sqlContext = new SQLContext(sp);
        H2OContext h2oContext = new H2OContext(sp.sc());
        h2oContext.start();

        HashSet<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        HashMap<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", kafkaBrokers);
        kafkaParams.put("auto.offset.reset", fromOffset);

        CraigslistJobTitlesApp staticApp = new CraigslistJobTitlesApp(craigslistJobTitles, sp.sc(), sqlContext, h2oContext);
        try {
             final Tuple2<Model<?, ?, ?>, Word2VecModel> tModel = staticApp.buildModels(craigslistJobTitles, "initialModel");
//            final Tuple2<Model<?, ?, ?>, Word2VecModel> tModel = importModels(h2oModelFolder, word2VecModelFolder, sp.sc());
//            final Model<?, ?, ?> tModel1 = importH2OModel(h2oModelFolder1);

            final String modelId = tModel._1()._key.toString();
            final Word2VecModel w2vModel = tModel._2();
            // exportModels(tModel._1(), w2vModel, sp.sc());

            // Create direct kafka stream with brokers and topics
            JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class,
                    StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

            // Classify incoming messages
            messages.map(mesage -> mesage._2()).filter(str -> !str.isEmpty())
                    .map(jobTitle -> staticApp.classify(jobTitle, modelId, w2vModel))
                    .map(pred -> new StringBuilder(100).append('\"').append(pred._1()).append("\" = ").append(Arrays.toString(pred._2())))
                    .print();

//            messages.map(mesage -> mesage._2()).filter(str -> !str.isEmpty())
//                    .map(jobTitle -> tModel1.score(new H2OFrame(jobTitle)))
//                    .map(pred -> pred._names)
//                    .print();

            jssc.start();
            jssc.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jssc.stop();
            staticApp.shutdown();
        }
    }

    private Tuple2<Model<?, ?, ?>, Word2VecModel> importModels(String h2oModelFolder, String word2VecModelFolder, SparkContext sc) {
        return new Tuple2<Model<?, ?, ?>, Word2VecModel>(importH2OModel(h2oModelFolder), Word2VecModel.load(sc, word2VecModelFolder));
    }

    private void exportModels(Model h2oModel, String h2oModelFolder, Word2VecModel w2vModel, String word2VecModelFolder, SparkContext sc) {
        exportH2OModel(h2oModel, h2oModelFolder);
        w2vModel.save(sc, word2VecModelFolder);
    }

    public void exportH2OModel(Model exportModel, String dir) {
        Model model = Handler.getFromDKV("model_id", exportModel._key, Model.class);

        List<Key> keysToExport = new LinkedList<>();
        keysToExport.add(model._key);
        keysToExport.addAll(model.getPublishedKeys());

        try {
            new ObjectTreeBinarySerializer().save(keysToExport, FileUtils.getURI(dir));
        } catch (IOException e) {
            throw new H2OIllegalArgumentException("dir", "exportModel", e);
        }
    }

    public static Model importH2OModel(String dir) {
        Model model = null;
        try {
            List<Key> importedKeys = new ObjectTreeBinarySerializer().load(FileUtils.getURI(dir));
            model = (Model) importedKeys.get(0).get();
        } catch (IOException e) {
            throw new H2OIllegalArgumentException("dir", "importModel", e);
        }

        return model;
    }
}
