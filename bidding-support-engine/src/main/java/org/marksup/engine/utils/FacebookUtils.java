package org.marksup.engine.utils;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.ReceiverInputDStream;

import com.restfb.batch.BatchRequest.BatchRequestBuilder;

public class FacebookUtils {

    public static ReceiverInputDStream<String> createStream(StreamingContext ssc, String accessToken,
            BatchRequestBuilder[] batchRequestBuilders) {
        return new FacebookInputDStream(ssc, accessToken, batchRequestBuilders, StorageLevel.MEMORY_AND_DISK_2());
    }

    public static ReceiverInputDStream<String> createStream(JavaStreamingContext jssc, String accessToken,
            BatchRequestBuilder[] batchRequestBuilders) {
        return new FacebookInputDStream(jssc.ssc(), accessToken, batchRequestBuilders, StorageLevel.MEMORY_AND_DISK_2());
    }
}
