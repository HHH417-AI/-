package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class WordCountSource{
    public static void main(String[] args) throws InterruptedException {
        // Kafka 配置
        Properties properties=new Properties();
        properties.put("bootstrap.servers", "master:9092,slave1:9092,slave2:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 创建 Kafka Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 随机生成单词和计数的列表
        String[] words = {"apple", "pie", "banana", "APPLE", "PIE","BANANA"};
        Random random = new Random();

        while (true) {
            // 随机选择一个单词和计数（1-5）
            String word = words[random.nextInt(words.length)];
            int count = random.nextInt(5) + 1;

            // 发送到 Kafka（格式：word,count）
            String message = word + "," + count;
            producer.send(new ProducerRecord<>("testtopic", message));
            System.out.println("Sent: " + message);

            // 模拟流式输入（间隔 5 秒）
            Thread.sleep(5000);
        }
    }


}
