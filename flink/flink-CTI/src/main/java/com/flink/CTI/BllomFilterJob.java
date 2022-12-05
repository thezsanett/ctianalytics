package com.flink.CTI;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.operators.util.BloomFilter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.json.JSONObject;

public class BllomFilterJob {





    /**
     * Skeleton for a Flink DataStream Job.
     *
     * <p>For a tutorial how to write a Flink application, check the
     * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
     *
     * <p>To package your application into a JAR file for execution, run
     * 'mvn clean package' on the command line.
     *
     * <p>If you change the name of the main class (with the public static void main(String[] args))
     * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
     */


    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        BloomFilter bloomfilter = new BloomFilter(1000,1000);



        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("alienvaultdata")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> records = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        DataStream<String> strings = records.map(new MapFunction<String,String>()
        {
            @Override
            public String map(String s) throws Exception {
                JSONObject a = new JSONObject(s);
                String ip = a.getString("indicator");
                bloomfilter.addHash(a.getString("indicator").hashCode() %1000);
                return a.getString("indicator");
            }
        });

        System.out.print(bloomfilter.testHash("59.127.15.941".hashCode()%1000));









        env.execute();


    }
}
