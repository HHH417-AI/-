package org.example;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //指定本地WEB-UI端口号
        conf.setInteger(RestOptions.PORT,9999);
        //1、创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);
        //2、从kafka读取文本流
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "master:9092,slave1:9092,slave2:9092");
        properties.setProperty("group.id", "consumer-group1");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "earliest");

        FlinkKafkaConsumer<String> wjh = new FlinkKafkaConsumer<>("topictest", new SimpleStringSchema(), properties);

        //配置数据源
        DataStreamSource<String> stream = env.addSource(wjh);

        //转换数据格式
        SingleOutputStreamOperator<Tuple2<String, Long>> returns = stream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                )
                .flatMap((String line, Collector<String> words) -> {
                    String[] tokens = line.split(" ");  // 将每行拆分为单词数组
                    if (tokens.length >= 1) {  // 检查是否有至少两个单词
                        words.collect(tokens[0]);  // 只收集第一个单词
                    }
                })
                .returns(Types.STRING) // 指定 flatMap 发射的元素类型为 String
                .map(word -> Tuple2.of(word, 1L)) // 将每个单词映射为 (word, 1) 的元组
                .returns(Types.TUPLE(Types.STRING, Types.LONG)) // 指定 map 发射的元素类型为 Tuple2<String, Long>
                .keyBy(t -> t.f0.toUpperCase())//分组
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))//5分钟的窗口
                .sum(1);//求和
//        returns.print();

        //输出到file

        // 定义文件输出的路径
        String outputPath = "./output";

        // 创建 FileSink
        FileSink<Tuple2<String, Long>> fileSink = FileSink
                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<Tuple2<String, Long>>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))  // 每 15 分钟滚动一次文件
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))  // 如果 5 分钟没有新数据，滚动文件
                                .withMaxPartSize(1024 * 1024 * 128)  // 文件最大大小为 128MB
                                .build()
                )
                .build();

        // 将统计结果写入文件
        returns.sinkTo(fileSink);


        //执行
        env.execute();


    }
}
