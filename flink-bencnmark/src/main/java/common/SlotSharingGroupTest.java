package common;

import junit.framework.TestCase;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SlotSharingGroupTest extends TestCase {
    public void  testSlotSharingGroup() throws Exception {

        Configuration configuration = new Configuration();
        configuration.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER,true);
        configuration.setInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY,7088);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1,configuration);
        env.getConfig().enableObjectReuse();

        DataStream<String> text = env.socketTextStream("localhost", 9992, "\n");

        SingleOutputStreamOperator<String> map = text.map(new MapFunction<String, String>() {

            @Override
            public String map(String value) throws Exception {
                return value;
            }
        });

        SingleOutputStreamOperator<String> filter = map.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return true;
            }
        }).slotSharingGroup("group_03");

        SingleOutputStreamOperator<String> bb = filter.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        });


        SingleOutputStreamOperator<String> cc = bb.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return true;
            }
        }).slotSharingGroup("group_04");

        SingleOutputStreamOperator<String> dd = cc.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        });

        dd.print();
        env.execute("xxx");
    }

}
