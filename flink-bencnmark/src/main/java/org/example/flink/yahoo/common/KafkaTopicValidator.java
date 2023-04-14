package org.example.flink.yahoo.common;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.example.flink.common.BenchmarkConfig;

import static org.example.flink.yahoo.AdvertisingTopologyFlinkWindows.kafkaSource;

/**
 * Simple util to print kafka partitions locally
 */
public class KafkaTopicValidator {
	public static void main(String[] args) throws Exception {
		final ParameterTool parameterTool = ParameterTool.fromArgs(args);
		BenchmarkConfig config = BenchmarkConfig.fromArgs(args);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setGlobalJobParameters(parameterTool);
		DataStream<String> rawMessageStream = env.addSource(kafkaSource(config));

		rawMessageStream.print();

		env.execute();
	}
}
