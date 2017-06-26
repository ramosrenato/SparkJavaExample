package com.renatoramos.java.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.Seconds;


/**
 * Created by renato on 25/06/2017.
 */
class StreamLogAnalysis {

    public void run() {

        String masterUrl = "local[*]";

        SparkConf conf = new SparkConf()
                .setAppName(WordCount.class.getName())
                .set("spark.driver.memory","4G")
                .set("spark.executor.memory","4G")
                .setMaster(masterUrl);

        JavaStreamingContext context = new JavaStreamingContext(conf, Seconds.apply(1));

        JavaReceiverInputDStream<String> lines = context.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER() );

        //lines.map(x => )


    }

}
