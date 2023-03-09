package org.example.flink.common;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;

import java.io.File;
import java.util.concurrent.CompletableFuture;

public class SubmitTool {
    public static void main(String[] args) {


        final ParameterTool params = ParameterTool.fromArgs(args);

        String jarFilePath = params.get("f", "/Users/zhangyang/experiment/CReSt/flink-bencnmark/target/flink-bencnmark-1.0-SNAPSHOT.jar");
        RestClusterClient<StandaloneClusterId> client;
        try {
            // 配置standalone集群信息
            Configuration config = new Configuration();
            String address = params.get("address", "localhost/6123/8081");
            String[] addressSplit = address.split("/");
            config.setString(JobManagerOptions.ADDRESS, addressSplit[0]);
            config.setInteger(JobManagerOptions.PORT, Integer.parseInt(addressSplit[1]));
            config.setInteger(RestOptions.PORT, Integer.parseInt(addressSplit[2]));
//            config.setString(PipelineOptions.NAME,"Filter Adults Job");
            client = new RestClusterClient<StandaloneClusterId>(config, StandaloneClusterId.getInstance());

            // Job运行的配置
            int parallelism = 1;
            SavepointRestoreSettings savePoint = SavepointRestoreSettings.none();

            //设置job的入口和参数
            File jarFile = new File(jarFilePath);
            PackagedProgram program = PackagedProgram
                    .newBuilder()
                    .setConfiguration(config)
                    .setJarFile(jarFile)
                    .setEntryPointClassName(params.get("q"))
                    .setSavepointRestoreSettings(savePoint)
                    .setArguments(args[args.length-1].split(" "))
                    .build();

            JobGraph jobGraph = PackagedProgramUtils.createJobGraph(program, config, parallelism, false);
//            JobVertex jobVertex = jobGraph.getVerticesAsArray()[0];
//            jobVertex.setResources();
//            jobVertex.setSlotSharingGroup();

            CompletableFuture<JobID> result = client.submitJob(jobGraph);
            JobID jobId = result.get();
            System.out.println("job: [" + jobId.toHexString() + "] 提交完成！");
            System.out.println("job: [" + jobId.toHexString() + "] 是否执行完成：" + result.isDone());
            System.out.println("job: [" + jobId.toHexString() + "] 是否异常结束：" + result.isCompletedExceptionally());
            System.out.println("job: [" + jobId.toHexString() + "] 是否取消：" + result.isCancelled());
            System.out.println(jobId.toHexString());
            while (client.getJobStatus(jobId).get().equals(JobStatus.RUNNING)) {
                break;
            }
            Thread.sleep(1000*60*10);
            client.cancel(jobId);
            CompletableFuture<JobDetailsInfo> jobDetails  = client.getJobDetails(jobId);
            JobDetailsInfo jobDetailsInfo = jobDetails.get();
            System.out.println(jobDetailsInfo.toString());
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
