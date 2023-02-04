package org.example.partition.entity;

import org.apache.flink.api.common.ExecutionConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AppConfig extends ExecutionConfig.GlobalJobParameters {
    private String name;

    private String mode;

    private Boolean isDebug;

    private List<String> localOperators;

    private Map<String, String> others;

    public AppConfig(String name, String mode, Boolean isDebug, List<String> localOperators, Map<String, String> others, List<String> remoteOperators, int localParallelism, int remoteParallelism, String localSlotGroup, String remoteSlotGroup) {
        this.name = name;
        this.mode = mode;
        this.isDebug = isDebug;
        this.localOperators = localOperators;
        this.others = others;
        this.remoteOperators = remoteOperators;
        this.localParallelism = localParallelism;
        this.remoteParallelism = remoteParallelism;
        this.localSlotGroup = localSlotGroup;
        this.remoteSlotGroup = remoteSlotGroup;
    }

    @Override
    public String toString() {
        return "AppConfig{" +
                "name='" + name + '\'' +
                ", mode='" + mode + '\'' +
                ", isDebug=" + isDebug +
                ", localOperators=" + localOperators +
                ", remoteOperators=" + remoteOperators +
                ", localParallelism=" + localParallelism +
                ", remoteParallelism=" + remoteParallelism +
                ", localSlotGroup='" + localSlotGroup + '\'' +
                ", remoteSlotGroup='" + remoteSlotGroup + '\'' +
                '}';
    }

    private List<String> remoteOperators;

    private int localParallelism;

    private int remoteParallelism;

    private String localSlotGroup;

    private String remoteSlotGroup;

    public AppConfig() {
    }



    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getLocalOperators() {
        return localOperators;
    }

    public void setLocalOperators(List<String> localOperators) {
        this.localOperators = localOperators;
    }

    public List<String> getRemoteOperators() {
        return remoteOperators;
    }

    public void setRemoteOperators(List<String> remoteOperators) {
        this.remoteOperators = remoteOperators;
    }

    public int getLocalParallelism() {
        return localParallelism;
    }

    public void setLocalParallelism(int localParallelism) {
        this.localParallelism = localParallelism;
    }

    public int getRemoteParallelism() {
        return remoteParallelism;
    }

    public void setRemoteParallelism(int remoteParallelism) {
        this.remoteParallelism = remoteParallelism;
    }

    public String getLocalSlotGroup() {
        return localSlotGroup;
    }

    public void setLocalSlotGroup(String localSlotGroup) {
        this.localSlotGroup = localSlotGroup;
    }

    public String getRemoteSlotGroup() {
        return remoteSlotGroup;
    }

    public void setRemoteSlotGroup(String remoteSlotGroup) {
        this.remoteSlotGroup = remoteSlotGroup;
    }

    public Map<String, String> getOthers() {
        return others;
    }

    public void setOthers(Map<String, String> others) {
        this.others = others;
    }

    @Override
    public Map<String, String> toMap() {
        Map<String, String> configMap = new HashMap<>();
        configMap.putAll(others);
        configMap.put("mode", mode);
        if (mode==null||!mode.equals("cross-region")) {
            return configMap;
        }
        configMap.put("name", name);
        configMap.put("localParallelism", String.valueOf(localParallelism));
        configMap.put("remoteParallelism", String.valueOf(remoteParallelism));
        configMap.put("localSlotGroup", localSlotGroup);
        configMap.put("remoteSlotGroup", remoteSlotGroup);
        configMap.put("localOperators", localOperators.toString());
        configMap.put("remoteOperators", remoteOperators.toString());
        if (isDebug!=null && isDebug) {
            configMap.put("isDebug", "true");
        } else {
            configMap.put("isDebug", "false");
        }

        return configMap;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public Boolean getDebug() {
        return isDebug;
    }

    public void setDebug(Boolean debug) {
        isDebug = debug;
    }
}
