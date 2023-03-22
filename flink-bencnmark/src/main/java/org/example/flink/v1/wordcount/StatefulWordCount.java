package org.example.flink.v1.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.util.Collector;
import org.example.flink.common.ConfigTool;
import org.example.flink.v1.nexmark.sinks.DummySink;
import org.example.flink.v1.wordcount.sources.RateControlledSourceFunction;
import org.example.partition.entity.AppConfig;

import java.util.Collection;

import static org.example.flink.common.ConfigTool.init;


public class StatefulWordCount {

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		final StreamExecutionEnvironment env = init(params);
		AppConfig jobConfiguration = (AppConfig) env.getConfig().getGlobalJobParameters();
//		SlotSharingGroup.newBuilder()
		final DataStream<String> text = env.addSource(
								new RateControlledSourceFunction(
										params.getInt("source-rate", 800000),
										params.getInt("sentence-size", 100))).uid("sentence-source");
		ConfigTool.configOperator(text, jobConfiguration);

		// split up the lines in pairs
		// (2-tuples) containing:
		// (word,1)
		DataStream<Tuple2<String, Long>> wordTuples = text.rebalance()
				.flatMap(new Tokenizer())
				.name("Splitter FlatMap")
				.uid("flatmap");
		ConfigTool.configOperator(wordTuples, jobConfiguration);

		DataStream<Tuple2<String, Long>> counts
				= wordTuples.keyBy(0)
				.flatMap(new CountWords())
				.name("Count")
				.uid("count");
		ConfigTool.configOperator(counts, jobConfiguration);

		GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
		// write to dummy sink
		SingleOutputStreamOperator<Object> latencySink = counts.transform("Latency Sink", objectTypeInfo,
						new DummySink<>())
				.uid("dummy-sink");
		ConfigTool.configOperator(latencySink,
				jobConfiguration);
		StreamGraph streamGraph = env.getStreamGraph(false);
		Collection<StreamNode> streamNodes = streamGraph.getStreamNodes();
		for (StreamNode streamNode:streamNodes) {
//			streamNode.setResources();
			if (jobConfiguration.getLocalOperators().contains(streamNode.getOperatorName())) {
				streamNode.setParallelism(jobConfiguration.getLocalParallelism());
				streamNode.setSlotSharingGroup(jobConfiguration.getLocalSlotGroup());
			} else {
				streamNode.setParallelism(jobConfiguration.getRemoteParallelism());
				streamNode.setSlotSharingGroup(jobConfiguration.getRemoteSlotGroup());
			}
		}
		// execute program
		env.execute(streamGraph);
//		if (params.has("job-name")) {
//			System.out.println(params.get("job-name"));
//			env.execute(params.get("job-name"));
//		} else {
//			env.execute("Stateful-WordCount_" + jobConfiguration.getMode() + "_" + UUID.randomUUID());
//		}
//		System.out.println(params.toMap());
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Long>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<>(token, 1L));
				}
			}
		}
	}

	public static final class CountWords extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, Long>> {

		private transient ReducingState<Long> count;

		@Override
		public void open(Configuration parameters) throws Exception {

			ReducingStateDescriptor<Long> descriptor =
					new ReducingStateDescriptor<Long>(
							"count", // the state name
							new Count(),
							BasicTypeInfo.LONG_TYPE_INFO);

			count = getRuntimeContext().getReducingState(descriptor);
		}

		@Override
		public void flatMap(Tuple2<String, Long> value, Collector<Tuple2<String, Long>> out) throws Exception {
			count.add(value.f1);
			out.collect(new Tuple2<>(value.f0, count.get()));
		}

		public static final class Count implements ReduceFunction<Long> {

			@Override
			public Long reduce(Long value1, Long value2) throws Exception {
				return value1 + value2;
			}
		}
	}

}
