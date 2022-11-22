package com.flink;


import org.apache.flink.connector.kafka.source.*;
import org.apache.flink.connector.kafka.source.enumerator.initializer.*;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.serialization.StringDeserializer;


public class App 
{       

  
       
    public static void main( String[] args )
    {
     // set up the execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    String consumerGroup = "Flink";
        String brokers = "broker";
    KafkaSource<String> source = KafkaSource.<String>builder()
    .setBootstrapServers(brokers)
    .setTopics("flink-input")
    .setGroupId(consumerGroup)
    .setStartingOffsets(OffsetsInitializer.earliest())
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
    .build();

    DataStream<String> inputstream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
    System.out.println(inputstream);

    try {
        env.execute();
    } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    }
    
}
}