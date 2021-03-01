import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class MySource implements SourceFunction<WaterSensor> {
    boolean flag = true;

    @Override
    public void run(SourceContext<WaterSensor> ctx) throws Exception {
        Random random = new Random();
        while (flag) {
            ctx.collect(
                    new WaterSensor(
                            "sensor_" + (random.nextInt(3) + 1),
                            System.currentTimeMillis(),
                            random.nextInt(10) + 40
                    )
            );
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}