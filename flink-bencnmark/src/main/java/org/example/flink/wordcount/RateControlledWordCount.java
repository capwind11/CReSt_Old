package org.example.flink.wordcount;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.example.flink.common.ConfigTool;
import org.example.flink.wordcount.sources.RateControlledSourceFunction;
import org.example.partition.entity.AppConfig;

import java.util.UUID;

import static org.example.flink.common.ConfigTool.configOperator;
import static org.example.flink.common.ConfigTool.init;

public class RateControlledWordCount {
	private final static int SENTENCE_SIZE = 100;

	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		final StreamExecutionEnvironment env = init(params);
		AppConfig jobConfiguartion = (AppConfig)env.getConfig().getGlobalJobParameters();

		final DataStream<String> text = env.addSource(
				new RateControlledSourceFunction(
						params.getInt("source-rate", 80000),
						params.getInt("sentence-size", 100)));

		ConfigTool.configOperator(text, jobConfiguartion);

		// split up the lines in pairs (2-tuples) containing:
		// (word,1)
		DataStream<Tuple2<String, Integer>> flatMapTokenizer = text
				.flatMap(new Tokenizer()).name("FlatMap tokenizer");;

		DataStream<Tuple2<String, Integer>>  counts = flatMapTokenizer.keyBy(0)
				.sum(1).name("Counts");
		// write to dummy sink
		DataStreamSink<Tuple2<String, Integer>> dummySink = counts.addSink(new SinkFunction<Tuple2<String, Integer>>() {
			private static final long serialVersionUID = 1L;

			public void invoke(Tuple2<String, Integer> value) {
				// nop
				int a = 1;
			}
		}).name("Dummy Sink");

		configOperator(dummySink, jobConfiguartion);

		// execute program
		JobExecutionResult res = env.execute("Rate-controlled-Streaming-WordCount_"+ jobConfiguartion.getMode() + "_" + UUID.randomUUID());
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
	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
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
