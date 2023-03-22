package org.example.flink.common;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.partition.entity.AppConfig;
import org.yaml.snakeyaml.Yaml;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;


//name: StatefulWordCount
//
//        resource_config:
//        local_parallelism: 1
//        remote_parallelism: 2
//        local_slot_group: local_group
//        remote_slot_group: remote_group
//
//        partition_config:
//        local_operator:
//        - sentence-source
//        remote_operator:
//        - flatmap
//        - count
//        - dummy-sink

public class ConfigTool {

    public static StreamExecutionEnvironment init(ParameterTool params) throws Exception {

        StreamExecutionEnvironment env;
        String mode = "normal";

        if (params.has("mode")) {
            mode = params.get("mode");
        }

        AppConfig appConfig;

        if (params.has("cfg")) {
            appConfig = loadPartitionConfig(params.get("cfg"));
            if (params.has("p1")) {
                appConfig.setLocalParallelism(params.getInt("p1"));
            }
            if (params.has("p2")) {
                appConfig.setRemoteParallelism(params.getInt("p2"));
            }
        } else if (mode.equals("cross-region")) {
            System.out.println("please specify the config file.");
            throw new Exception("no config file specified under cross-region mode");
        } else {
            appConfig = new AppConfig();
            appConfig.setMode(mode);
        }
        Configuration configuration = new Configuration();
//            Configuration conf = new Configuration();
//            StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        // 定义一个配置 import org.apache.flink.configuration.Configuration;包下
//            Configuration configuration = new Configuration();

// 指定本地WEB-UI端口号
        configuration.setInteger(RestOptions.PORT, 8081);
        if ((appConfig.getDebug() != null && appConfig.getDebug()) || (params.has("debug") && params.getBoolean("debug"))) {

            configuration.setString("taskmanager.numberOfTaskSlots", "12");
// 执行环境使用当前配置
            env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        } else {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        }
        env.setParallelism(params.getInt("p", 1));

        if (appConfig.getMode().equals("offline")) {
            env.disableOperatorChaining();
        }

        appConfig.setOthers(params.toMap());
//        appConfig.toMap();
        env.getConfig().setGlobalJobParameters(appConfig);
        // mode: normal, cross-region, offline
        return env;
    }

    public static AppConfig loadPartitionConfig(String fileName) throws FileNotFoundException {

        BufferedReader br = new BufferedReader(new FileReader(fileName));

        Yaml yaml = new Yaml();
        Map<String, Object> config = (Map<String, Object>) yaml.load(br);

        AppConfig appConfig = JSON.parseObject(JSON.toJSONString(config), AppConfig.class);

        return appConfig;
    }

    public static void configOperator(DataStream operator, AppConfig appConfig) {
        return;
//        String mode = appConfig.getMode();
//        if (mode.equals("cross-region")) {
//            if (appConfig.getLocalOperators().contains(operator.getTransformation().getUid())) {
//                operator.getTransformation().setParallelism(appConfig.getLocalParallelism());
//                operator.getTransformation().setSlotSharingGroup(appConfig.getLocalSlotGroup());
//            } else {
//                operator.getTransformation().setParallelism(appConfig.getRemoteParallelism());
//                operator.getTransformation().setSlotSharingGroup(appConfig.getRemoteSlotGroup());
//            }
//        }
    }

    public static void configOperator(DataStreamSink<Tuple2<String, Integer>> operator, AppConfig appConfig) {

        String mode = appConfig.getMode();
        if (mode.equals("cross-region")) {
            if (appConfig.getLocalOperators().contains(operator.getTransformation().getUid())) {
                operator.getTransformation().setParallelism(appConfig.getLocalParallelism());
                operator.getTransformation().setSlotSharingGroup(appConfig.getLocalSlotGroup());
            } else {
                operator.getTransformation().setParallelism(appConfig.getRemoteParallelism());
                operator.getTransformation().setSlotSharingGroup(appConfig.getRemoteSlotGroup());
            }
        }
    }
}
