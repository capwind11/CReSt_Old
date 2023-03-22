package org.example.flink.common;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConfigToolV2 extends ExecutionConfig.GlobalJobParameters {

    private String mode;

    private String className;

    private Map<String, Object> jobConfiguration;

    public ConfigToolV2(String[] args) throws FileNotFoundException {

        ParameterTool parameters = ParameterTool.fromArgs(args);
        this.mode = parameters.get("mode", "offline");
        boolean isDebug = parameters.getBoolean("debug", true);
        if (parameters.has("config")) {
            Yaml yml = new Yaml(new SafeConstructor());
            this.jobConfiguration = (Map) yml.load(new FileInputStream(parameters.get("config")));
            if (this.jobConfiguration.containsKey("mode")) {
                this.mode = (String) this.jobConfiguration.get("mode");
            }
        } else {
            this.jobConfiguration = new HashMap<>();
            this.jobConfiguration.put("mode",  this.mode);
        }
        this.jobConfiguration.put("debug", isDebug);

        this.className = new Exception().getStackTrace()[1].getClassName();
        this.className = this.className.substring(this.className.lastIndexOf('.')+1);
    }

    public StreamExecutionEnvironment setUpEvironment() throws FileNotFoundException {

        StreamExecutionEnvironment env;
        Configuration configuration = new Configuration();
        if ((Boolean) this.jobConfiguration.getOrDefault("debug", true)) {
            configuration.setInteger(RestOptions.PORT, (int) this.jobConfiguration.getOrDefault("rest.port", 8082));
//            configuration.setString("taskmanager.numberOfTaskSlots", String.valueOf(this.jobConfiguration.getOrDefault("parallelism", 1)));
        }

        env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism((int) this.jobConfiguration.getOrDefault("parallelism", 1));

        env.getConfig().setGlobalJobParameters(this);
        if (this.mode.equals("offline")) {
            env.disableOperatorChaining();
        }
        return env;
    }

    public void reconfigurationAndSubmit(StreamExecutionEnvironment env) throws Exception {

        HashMap<String, Integer> taskNameMap = new HashMap<String, Integer>();
        String jobName = (String) this.jobConfiguration.getOrDefault("job_name", className+"_"+this.mode);
        StreamGraph streamGraph = env.getStreamGraph(false);
        Collection<StreamNode> streamNodes = streamGraph.getStreamNodes();
        for (StreamNode streamNode:streamNodes) {
            String operatorDescription = streamNode.getOperatorDescription();
            if (!taskNameMap.containsKey(operatorDescription)) {
                taskNameMap.put(operatorDescription, 0);
            }
            streamNode.setOperatorDescription(operatorDescription +"-"+taskNameMap.get(operatorDescription));
            taskNameMap.put(operatorDescription, taskNameMap.get(operatorDescription)+1);
        }

        if (this.mode.equals("cross-region")) {
            List<String> localOpearators = (List<String>) this.jobConfiguration.get("local_operators");
            List<String> remoteOpearators = (List<String>) this.jobConfiguration.get("remote_operators");
            int localParallelism = (int) this.jobConfiguration.get("local_parallelism");
            int remoteParallelism = (int) this.jobConfiguration.get("remote_parallelism");;
            for (StreamNode streamNode:streamNodes) {
                if (localOpearators.contains(streamNode.getOperatorName())) {
                    streamNode.setParallelism(localParallelism);
                    streamNode.setSlotSharingGroup("local");
                } else {
                    streamNode.setParallelism(remoteParallelism);
                    streamNode.setSlotSharingGroup("remote");
                }
            }
        }

        streamGraph.setJobName(jobName);
        env.execute(streamGraph);
    }

    @Override
    public Map<String, String> toMap() {
        Map<String, String> configMap = new HashMap<>();
        for (Map.Entry<String,Object> e : this.jobConfiguration.entrySet()) {
            {
                configMap.put(e.getKey(), e.getValue().toString());
            }
        }
        return configMap;
    }


}
