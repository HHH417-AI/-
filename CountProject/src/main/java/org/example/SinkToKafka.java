package org.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class SinkToKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties=new Properties();
        properties.setProperty("bootstrap.servers", "master:9092,slave1:9092,slave2:9092");
        DataStreamSource<Event> eventDataStreamSource = env.addSource(new ClickSource());
        SingleOutputStreamOperator<String> map = eventDataStreamSource.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event event) throws Exception {
                return event.toString();
            }
        });


        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<String>(
                "topictest",                  // 目标 topic
                new SimpleStringSchema(),     // 序列化 schema
                properties                 // producer 配置
        ); // 容错

        map.addSink(myProducer);


        env.execute();
    }
}
