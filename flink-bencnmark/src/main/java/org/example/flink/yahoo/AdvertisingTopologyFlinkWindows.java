/**
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package org.example.flink.yahoo;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.example.flink.common.BenchmarkConfig;
import org.example.flink.common.ConfigToolV2;
import org.example.flink.v1.nexmark.sinks.DummySink;
import org.example.flink.yahoo.common.RedisAdCampaignCache;
import org.example.flink.yahoo.common.ThroughputLogger;
import org.example.flink.yahoo.generator.EventGeneratorSource;
import org.example.flink.yahoo.generator.RedisHelper;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * To Run:  flink run -c flink.benchmark.AdvertisingTopologyFlinkWindows target/flink-benchmarks-0.1.0.jar "../conf/benchmarkConf.yaml"
 *
 * This job variant uses Flinks built-in windowing and triggering support to compute the windows
 * and trigger when each window is complete as well as once per second.
 */
public class AdvertisingTopologyFlinkWindows {

  private static final Logger LOG = LoggerFactory.getLogger(AdvertisingTopologyFlinkWindows.class);

  public static void main(final String[] args) throws Exception {

    BenchmarkConfig config = BenchmarkConfig.fromArgs(args);
    ConfigToolV2 configToolV2 = new ConfigToolV2(args);
    StreamExecutionEnvironment env = setupEnvironment(config);
//    env.disableOperatorChaining();
//    env.setParallelism(1);
    DataStream<String> rawMessageStream = streamSource(config, env);

    // rawMessageStream.getTransformation().setSlotSharingGroup("local");
    // log performance
    rawMessageStream.flatMap(new ThroughputLogger<String>(240, 1_000_000));

    DataStream<Tuple2<String, String>> joinedAdImpressions = rawMessageStream
      .flatMap(new DeserializeBolt()).name("Flat Map-0")
      .filter(new EventFilterBolt())
      .<Tuple2<String, String>>project(2, 5) //ad_id, event_time
      .flatMap(new RedisJoinBolt(config)).name("Flat Map-1") // campaign_id, event_time
      .assignTimestampsAndWatermarks(new AdTimestampExtractor()); // extract timestamps and generate watermarks from event_time

    WindowedStream<Tuple3<String, String, Long>, Tuple, TimeWindow> windowStream = joinedAdImpressions
      .map(new MapToImpressionCount())
      .keyBy(0) // campaign_id
      .timeWindow(Time.of(config.windowSize, TimeUnit.MILLISECONDS));

    // set a custom trigger
    windowStream.trigger(new EventAndProcessingTimeTrigger());

    // campaign_id, window end time, count
    DataStream<Tuple3<String, String, Long>> result =
      windowStream.apply(sumReduceFunction(), sumWindowFunction());

    // write result to redis
//    result.addSink(new RedisResultSinkOptimized(config));
//    if (config.getParameters().has("add.result.sink.optimized")) {
//      result.addSink(new RedisResultSinkOptimized(config));
//    } else {
////      result.addSink(new DummySink<>());
//      result.addSink(new RedisResultSink(config));
//    }
    result.addSink(new SinkFunction<Tuple3<String, String, Long>>() {

      @Override
      public void invoke(Tuple3<String, String, Long> value, Context ctx) throws Exception {}
    });
    configToolV2.reconfigurationAndSubmit(env);
  }

  /**
   * Choose source - either Kafka or data generator
   */
  private static DataStream<String> streamSource(BenchmarkConfig config, StreamExecutionEnvironment env) {
    // Choose a source -- Either local generator or Kafka
    RichParallelSourceFunction<String> source;
    String sourceName;
    if (config.useLocalEventGenerator) {
      EventGeneratorSource eventGenerator = new EventGeneratorSource(config);
      source = eventGenerator;
      sourceName = "EventGenerator";

      Map<String, List<String>> campaigns = eventGenerator.getCampaigns();
      RedisHelper redisHelper = new RedisHelper(config);
      redisHelper.prepareRedis(campaigns);
      redisHelper.writeCampaignFile(campaigns);
    } else {
      source = kafkaSource(config);
      sourceName = "Kafka";
    }

    return env.addSource(source, sourceName).disableChaining();
  }

  /**
   * Setup Flink environment
   */
  private static StreamExecutionEnvironment setupEnvironment(BenchmarkConfig config) {
    Configuration configuration = new Configuration();
//    configuration.setInteger(RestOptions.PORT, 8082);
//    env.set
    configuration.setString("taskmanager.numberOfTaskSlots", "9");
    configuration.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
    configuration.setInteger(RestOptions.PORT, 8082);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
    env.getConfig().setGlobalJobParameters(config.getParameters());

//    if (config.checkpointsEnabled) {
//      env.enableCheckpointing(config.checkpointInterval);
//    }

    // use event time
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    return env;
  }

  /**
   * Sum - window reduce function
   */
  private static ReduceFunction<Tuple3<String, String, Long>> sumReduceFunction() {
    return new ReduceFunction<Tuple3<String, String, Long>>() {
      @Override
      public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> t0, Tuple3<String, String, Long> t1) throws Exception {
        t0.f2 += t1.f2;
        return t0;
      }
    };
  }

  /**
   * Sum - Window function, summing already happened in reduce function
   */
  private static WindowFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, Tuple, TimeWindow> sumWindowFunction() {
    return new WindowFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, Tuple, TimeWindow>() {
      @Override
      public void apply(Tuple keyTuple, TimeWindow window, Iterable<Tuple3<String, String, Long>> values, Collector<Tuple3<String, String, Long>> out) throws Exception {
        Iterator<Tuple3<String, String, Long>> valIter = values.iterator();
        Tuple3<String, String, Long> tuple = valIter.next();
        if (valIter.hasNext()) {
          throw new IllegalStateException("Unexpected");
        }
        tuple.f1 = Long.toString(window.getEnd());
        out.collect(tuple); // collect end time here
      }
    };
  }

  /**
   * Configure Kafka source
   */
  private static FlinkKafkaConsumer<String> kafkaSource(BenchmarkConfig config) {
    return new FlinkKafkaConsumer<>(
      config.kafkaTopic,
      new SimpleStringSchema(),
      config.getParameters().getProperties());
  }

  /**
   * Custom trigger - Fire and purge window when window closes, also fire every 1000 ms.
   */
  private static class EventAndProcessingTimeTrigger extends Trigger<Object, TimeWindow> {

    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
      ctx.registerEventTimeTimer(window.maxTimestamp());
      // register system timer only for the first time
      ValueState<Boolean> firstTimerSet = ctx.getKeyValueState("firstTimerSet", Boolean.class, false);
      if (!firstTimerSet.value()) {
        ctx.registerProcessingTimeTimer(System.currentTimeMillis() + 1000L);
        firstTimerSet.update(true);
      }
      return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
      return TriggerResult.FIRE_AND_PURGE;
    }

    @Override
    public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
      // schedule next timer
      ctx.registerProcessingTimeTimer(System.currentTimeMillis() + 1000L);
      return TriggerResult.FIRE;
    }
  }

  /**
   * Parse JSON
   */
  private static class DeserializeBolt implements
    FlatMapFunction<String, Tuple7<String, String, String, String, String, String, String>> {

    transient JSONParser parser = null;

    @Override
    public void flatMap(String input, Collector<Tuple7<String, String, String, String, String, String, String>> out)
      throws Exception {
      if (parser == null) {
        parser = new JSONParser();
      }
      JSONObject obj = (JSONObject) parser.parse(input);

      Tuple7<String, String, String, String, String, String, String> tuple =
        new Tuple7<>(
          obj.getAsString("user_id"),
          obj.getAsString("page_id"),
          obj.getAsString("ad_id"),
          obj.getAsString("ad_type"),
          obj.getAsString("event_type"),
          obj.getAsString("event_time"),
          obj.getAsString("ip_address"));
      out.collect(tuple);
    }
  }

  /**
   * Filter out all but "view" events
   */
  public static class EventFilterBolt implements
    FilterFunction<Tuple7<String, String, String, String, String, String, String>> {
    @Override
    public boolean filter(Tuple7<String, String, String, String, String, String, String> tuple) throws Exception {
      return tuple.getField(4).equals("view");
    }
  }

  /**
   * Map ad ids to campaigns using cached data from Redis
   */
  private static final class RedisJoinBolt extends RichFlatMapFunction<Tuple2<String, String>, Tuple2<String, String>> {

    private RedisAdCampaignCache redisAdCampaignCache;
    private BenchmarkConfig config;

    public RedisJoinBolt(BenchmarkConfig config) {
      this.config = config;
    }

    @Override
    public void open(Configuration parameters) {
      //initialize jedis
      String redis_host = config.redisHost;
      LOG.info("Opening connection with Jedis to {}", redis_host);
      this.redisAdCampaignCache = new RedisAdCampaignCache(redis_host);
      this.redisAdCampaignCache.prepare();
    }

    @Override
    public void flatMap(Tuple2<String, String> input, Collector<Tuple2<String, String>> out) throws Exception {
      String ad_id = input.getField(0);
      String campaign_id = this.redisAdCampaignCache.execute(ad_id);
      if (campaign_id == null) {
        return;
      }

      Tuple2<String, String> tuple = new Tuple2<>(campaign_id, (String) input.getField(1)); // event_time
      out.collect(tuple);
    }
  }


  /**
   * Generate timestamp and watermarks for data stream
   */
  private static class AdTimestampExtractor implements AssignerWithPeriodicWatermarks<Tuple2<String, String>> {

    long maxTimestampSeen = 0;

    @Override
    public long extractTimestamp(Tuple2<String, String> element, long currentTimestamp) {
      long timestamp = Long.parseLong(element.f1);
      maxTimestampSeen = Math.max(timestamp, maxTimestampSeen);
      return timestamp;
    }

//    @Override
//    public long extractWatermark(Tuple2<String, String> element, long currentTimestamp) {
//      return Long.MIN_VALUE;
//    }

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
      return new Watermark(maxTimestampSeen - 1L);
    }
  }

  /**
   *
   */
  private static class MapToImpressionCount implements MapFunction<Tuple2<String, String>, Tuple3<String, String, Long>> {
    @Override
    public Tuple3<String, String, Long> map(Tuple2<String, String> t3) throws Exception {
      return new Tuple3<>(t3.f0, t3.f1, 1L);
    }
  }

  /**
   * Sink computed windows to Redis
   */
  private static class RedisResultSink extends RichSinkFunction<Tuple3<String, String, Long>> {
    private Jedis flushJedis;

    private BenchmarkConfig config;

    public RedisResultSink(BenchmarkConfig config) {
      this.config = config;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      flushJedis = new Jedis(config.redisHost);
    }

    @Override
    public void invoke(Tuple3<String, String, Long> result) throws Exception {
      // set (campaign, count)
      //    flushJedis.hset("campaign-counts", result.f0, Long.toString(result.f2));

      String campaign = result.f0;
      String timestamp = result.f1;
      String windowUUID = getOrCreateWindow(campaign, timestamp);

      flushJedis.hset(windowUUID, "seen_count", Long.toString(result.f2));
      flushJedis.hset(windowUUID, "time_updated", Long.toString(System.currentTimeMillis()));
      flushJedis.lpush("time_updated", Long.toString(System.currentTimeMillis()));
    }

    private String getOrCreateWindow(String campaign, String timestamp) {
      String windowUUID = flushJedis.hmget(campaign, timestamp).get(0);
      if (windowUUID == null) {
        windowUUID = UUID.randomUUID().toString();
        flushJedis.hset(campaign, timestamp, windowUUID);
        getOrCreateWindowList(campaign, timestamp);
      }
      return windowUUID;
    }

    private void getOrCreateWindowList(String campaign, String timestamp) {
      String windowListUUID = flushJedis.hmget(campaign, "windows").get(0);
      if (windowListUUID == null) {
        windowListUUID = UUID.randomUUID().toString();
        flushJedis.hset(campaign, "windows", windowListUUID);
      }
      flushJedis.lpush(windowListUUID, timestamp);
    }

    @Override
    public void close() throws Exception {
      super.close();
      flushJedis.close();
    }
  }

  /**
   * Simplified version of Redis data structure
   */
  private static class RedisResultSinkOptimized extends RichSinkFunction<Tuple3<String, String, Long>> {
    private final BenchmarkConfig config;
    private Jedis flushJedis;

    public RedisResultSinkOptimized(BenchmarkConfig config){
      this.config = config;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      flushJedis = new Jedis(config.redisHost);
      flushJedis.select(1); // select db 1
    }

    @Override
    public void invoke(Tuple3<String, String, Long> result) throws Exception {
      // set campaign id -> (window-timestamp, count)
      flushJedis.hset(result.f0, result.f1, Long.toString(result.f2));
    }

    @Override
    public void close() throws Exception {
      super.close();
      flushJedis.close();
    }
  }
}
