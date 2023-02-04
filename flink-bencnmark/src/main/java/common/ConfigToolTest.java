package common;

import junit.framework.TestCase;
import org.example.flink.common.ConfigTool;
import org.example.partition.entity.AppConfig;

import java.io.FileNotFoundException;

public class ConfigToolTest extends TestCase {
    public void testLoadPartitionConfig() throws FileNotFoundException {

        AppConfig appConfig = ConfigTool.loadPartitionConfig("config/StatefulWordCount.yaml");
        System.out.printf(appConfig.toString());

    }

    public void testInit() {

    }

    public void testConfigOperator() {
    }
}