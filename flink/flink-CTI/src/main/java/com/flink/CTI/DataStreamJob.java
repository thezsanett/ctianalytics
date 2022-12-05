package com.flink.CTI;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.json.JSONObject;

public class DataStreamJob {

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
	    final String bootstrapServers = args.length > 0 ? args[0] : "broker:9092";

		// Set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		SpaceSaving spaceSaving = new SpaceSaving();
		spaceSaving.initSpaceSaving(10);

		KafkaSource<String> source = KafkaSource.<String>builder()
				.setBootstrapServers("broker:9092")
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

				return a.getString("indicator");
			}
		});

		strings.map(new MapFunction<String, String>() {
			@Override
			public String map(String s) throws Exception {
				spaceSaving.updateSpaceSaving(s);
				spaceSaving.query();
				return s;
			}
		});
		// spaceSaving.updateSpaceSaving();

		env.execute();
	}

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into multiple pairs in the
     * form of "(word,1)" ({@code Tuple2<String, Integer>}).
     */
    public static final class Tokenizer
            implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // Normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // Emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }

    // Implements a simple reducer using FlatMap to
    // reduce the Tuple2 into a single string for 
    // writing to kafka topics
    public static final class Reducer
            implements FlatMapFunction<Tuple2<String, Integer>, String> {

        @Override
        public void flatMap(Tuple2<String, Integer> value, Collector<String> out) {
        	// Convert the pairs to a string
        	// for easy writing to Kafka Topic
        	String count = value.f0 + " " + value.f1;
        	out.collect(count);
        }
    }
}