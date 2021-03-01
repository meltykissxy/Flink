import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
public class Flink01_WC_Batch {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 1.读取文件
        DataSource<String> lineDS = env.readTextFile("data/word.txt");
        // 2.转换数据格式
        FlatMapOperator<String, Tuple2<String, Integer>> wordsAndOne = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        });

        // 3.按照word进行分组
        UnsortedGrouping<Tuple2<String, Integer>> wordAndOneGS = wordsAndOne.groupBy(0);

        // 4.分组内聚合统计
        AggregateOperator<Tuple2<String, Integer>> sum = wordAndOneGS.sum(1);

        // 5.打印结果
        sum.print();
    }
}