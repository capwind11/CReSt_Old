package org.example.flink.wordcount;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.example.flink.common.ConfigTool;
import org.example.flink.wordcount.sources.RateControlledSourceFunction;
import org.example.partition.entity.AppConfig;

import java.util.UUID;

import static org.example.flink.common.ConfigTool.init;

public class TwoInputsWordCount {

	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		final StreamExecutionEnvironment env = init(params);
		AppConfig jobConfiguartion = (AppConfig) env.getConfig().getGlobalJobParameters();

		final DataStream<String> textOne = env.addSource(
						new RateControlledSourceFunction(
								params.getInt("source-rate", 80000),
								params.getInt("sentence-size", 100)))
				.name("Source One");
		ConfigTool.configOperator(textOne, jobConfiguartion);

		final DataStream<String> textTwo = env.addSource(
						new RateControlledSourceFunction(
								params.getInt("source-rate", 80000),
								params.getInt("sentence-size", 100)))
				.name("Source Two");
		ConfigTool.configOperator(textTwo, jobConfiguartion);

		// split up the lines in pairs (2-tuples) containing:
		// (word,1)
		DataStream<Tuple2<String, Integer>> flatMapTokenizer = textOne.connect(textTwo)
				.flatMap(new Tokenizer())
				.name("FlatMap tokenizer");
		ConfigTool.configOperator(flatMapTokenizer, jobConfiguartion);

		SingleOutputStreamOperator<Tuple2<String, Integer>> counts = flatMapTokenizer.keyBy(0)
				.sum(1)
				.name("Count op");
		ConfigTool.configOperator(counts, jobConfiguartion);

		// write to dummy sink
		DataStreamSink<Tuple2<String, Integer>> dummySink = counts.addSink(new SinkFunction<Tuple2<String, Integer>>() {
					private static final long serialVersionUID = 1L;

					public void invoke(Tuple2<String, Integer> value) {
						// nop
					}
				})
				.name("Dummy Sink");
		ConfigTool.configOperator(dummySink, jobConfiguartion);

		// execute program
		JobExecutionResult res = env.execute("Two-Input-Streaming-WordCount_" + jobConfiguartion.getMode() + "_" + UUID.randomUUID());
//		System.err.println("Execution time: " + res.getNetRuntime());
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	/**
	 * Implements the string tokenizer that splits sentences into words as a
	 * user-defined FlatMapFunction. The function takes a line (String) and splits
	 * it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
	 * Integer>}).
	 */
	public static final class Tokenizer implements CoFlatMapFunction<String, String, Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap1(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
			tokenize(value, out);
		}

		@Override
		public void flatMap2(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
			tokenize(value, out);
		}

		private void tokenize(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<>(token, 1));
				}
			}
		}
	}

}
