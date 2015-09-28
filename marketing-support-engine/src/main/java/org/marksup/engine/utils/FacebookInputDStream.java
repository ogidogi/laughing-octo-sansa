package org.marksup.engine.utils;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.dstream.ReceiverInputDStream;
import org.apache.spark.streaming.receiver.Receiver;

import com.restfb.DefaultFacebookClient;
import com.restfb.FacebookClient;
import com.restfb.Version;
import com.restfb.batch.BatchRequest.BatchRequestBuilder;

public class FacebookInputDStream extends ReceiverInputDStream<String> {

    private String accessToken;
    private BatchRequestBuilder[] batchRequestBuilders;
    private StorageLevel storageLevel;

    public FacebookInputDStream(StreamingContext ssc, String accessToken, BatchRequestBuilder[] batchRequestBuilders,
            StorageLevel storageLevel) {
        super(ssc, scala.reflect.ClassTag$.MODULE$.apply(String.class));
        this.accessToken = accessToken;
        this.storageLevel = storageLevel;
        this.batchRequestBuilders = batchRequestBuilders;
    }

    @Override
    public Receiver<String> getReceiver() {
        return new FacebookReceiver(accessToken, batchRequestBuilders, storageLevel);
    }

    private class FacebookReceiver extends Receiver<String> {

        private static final long serialVersionUID = -1343295871823035254L;

        private volatile FacebookClient facebookClient;
        private volatile boolean stopped = false;

        public FacebookReceiver(String accessToken, BatchRequestBuilder[] batchRequestBuilders, StorageLevel storageLevel) {
            super(storageLevel);
        }

        @Override
        public void onStart() {
            facebookClient = new DefaultFacebookClient(accessToken, Version.VERSION_2_4);
            // Until stopped or connection broken continue reading
            while (!stopped && (facebookClient != null)) {
                facebookClient.executeBatch(Arrays.asList(batchRequestBuilders).stream().map(b -> b.build()).collect(Collectors.toList()))
                        .forEach(batch -> store(batch.getBody()));
            }
        }

        @Override
        public void onStop() {
            stopped = true;
            setFacebookClient(null);
        }

        private synchronized void setFacebookClient(FacebookClient facebookClient) {
            if (facebookClient != null) {
                // TODO shutdown
            }
            this.facebookClient = facebookClient;
        }
    }
}
