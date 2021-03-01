import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public class MyRichMapFunction extends RichMapFunction<String, WaterSensor> {
    @Override
    public WaterSensor map(String value) throws Exception {
        String[] datas = value.split(",");
        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("open....");
    }

    @Override
    public void close() throws Exception {
        System.out.println("close....");
    }

}
