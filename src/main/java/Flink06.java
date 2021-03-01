import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink06 {
    public static void main(String[] args) {
//        // 0.创建执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//
//        // 1.读取数据
//        SingleOutputStreamOperator<WaterSensor> sensorDS = ReadAsWaterSensor.getWaterSensorFromFile(env);
//
//        // 2.转换成元组
//        SingleOutputStreamOperator<Tuple2<String, Integer>> sensorTuple = sensorDS
//                .map((MapFunction<WaterSensor, Tuple2<String, Integer>>) r -> new Tuple2<String, Integer>(r.getId(), r.getVc()))
//                .returns(new TypeHint<Tuple2<String, Integer>>() {});
//
//
//        // 3.按照id分组
//        KeyedStream<Tuple2<String, Integer>, String> sensorKS = sensorTuple.keyBy(r -> r.f0);
//
//        // 4.聚合
//        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDS = sensorKS.reduce((v1, v2) -> new Tuple2<String, Integer>(v1.f0, v1.f1 + v2.f1));
//
//        resultDS.print("reduce");
//
//        env.execute();
    }
}
