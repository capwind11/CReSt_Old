package org.example.flink.yahoo.common;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.example.flink.common.BenchmarkConfig;
import org.example.flink.yahoo.generator.EventGeneratorSource;
import org.example.flink.yahoo.generator.RedisHelper;

import java.util.List;
import java.util.Map;

/**
 * Distributed Data Generator for AdImpression Events.
 *
 *
 * (by default) we generate 100 campaigns, with 10 ads each.
 * We write those 1000 ads into Redis, with ad_is --> campaign_id
 *
 *
 *
 *
 */
public class AdImpressionsGenerator {

	public static void main(String[] args) throws Exception {

    BenchmarkConfig benchmarkConfig = BenchmarkConfig.fromArgs(args);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setGlobalJobParameters(benchmarkConfig.getParameters());
//    env.setParallelism(10);
    EventGeneratorSource eventGenerator = new EventGeneratorSource(benchmarkConfig);

    Map<String, List<String>> campaigns = eventGenerator.getCampaigns();
    RedisHelper redisHelper = new RedisHelper(benchmarkConfig);
    redisHelper.prepareRedis(campaigns);
    redisHelper.writeCampaignFile(campaigns);

    DataStream<String> adImpressions = env.addSource(eventGenerator);

//		adImpressions.flatMap(new ThroughputLogger<String>(240, 1_000));

    adImpressions.addSink(new FlinkKafkaProducer<>(
      benchmarkConfig.kafkaTopic,
      new SimpleStringSchema(),
      benchmarkConfig.getParameters().getProperties()
    ));
    env.disableOperatorChaining();

		env.execute("Ad Impressions data generator " + benchmarkConfig.getParameters().toMap().toString());
	}


}
