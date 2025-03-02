package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaWordCountFlink {
    public static void main(String[] args) throws Exception {
        // 创建 Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka 消费者配置
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092,slave1:9092,slave2:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "flink-consumer-group");

        // 创建 Kafka 数据源
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "testtopic",
                new SimpleStringSchema(),
                properties
        );

        // 添加数据源
        DataStream<String> stream = env.addSource(kafkaConsumer)
                .flatMap(new Tokenizer())  // 解析为 (小写单词, count)
                .keyBy(t -> t.f0)          // 按单词分组
                .window(TumblingProcessingTimeWindows.of(Time.minutes(5))) // 5分钟滚动窗口
                .process(new WindowCounter()) // 统计并输出窗口时间
                ;

        //输出到file

        // 定义文件输出的路径
        String outputPath = "./output";

        // 创建 FileSink
        FileSink<String> fileSink = FileSink
                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(5))  // 每 5 分钟滚动一次文件
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))  // 如果 5 分钟没有新数据，滚动文件
                                .withMaxPartSize(1024 * 1024 * 128)  // 文件最大大小为 128MB
                                .build()
                )
                .build();

        // 将统计结果写入文件
        stream.sinkTo(fileSink);

        env.execute("Kafka Windowed WordCount");
    }

    // 解析输入数据为 (小写单词, count)
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] parts = value.split(",");//以逗号分割
            String word = parts[0].toLowerCase(); // 大小写不敏感
            int count = Integer.parseInt(parts[1]);//计数
            out.collect(Tuple2.of(word, count));
        }
    }

    // 统计窗口内的计数，并添加触发时间
    public static class WindowCounter extends ProcessWindowFunction<
            Tuple2<String, Integer>, String, String, TimeWindow> {

        private static final DateTimeFormatter formatter =
                DateTimeFormatter.ofPattern("yyyy/MM/dd'T'HH:mm:ss").withZone(ZoneId.systemDefault());//格式为年月日时分秒

        @Override
        public void process(
                String key,
                Context context,
                Iterable<Tuple2<String, Integer>> elements,
                Collector<String> out) {

            // 计算总计数
            int sum = 0;
            for (Tuple2<String, Integer> t : elements) {
                sum += t.f1;
            };

            // 获取窗口结束时间（触发时间）
            long windowEnd = context.window().getEnd();
            String triggerTime = formatter.format(Instant.ofEpochMilli(windowEnd));

            // 输出格式：触发时间,单词,总计数
            out.collect(String.format("%s,%s,%d", triggerTime, key, sum));
        }
    }
}
