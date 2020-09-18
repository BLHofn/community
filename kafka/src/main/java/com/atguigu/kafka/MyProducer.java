package com.atguigu.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class MyProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        //kafka集群，broker-list
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop101:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        //重试次数
        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        //批次大小
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        //等待时间
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        //RecordAccumulator缓冲区大小
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
//        对key和value进行序列化操作
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

//        创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
//        生产消息
        for (int i = 0; i < 10; i++) {
//            指定分区
//            producer.send(new ProducerRecord<>("first",0,"xss","atguigu"+i));
//            使用黏性分区器
            System.out.println("发送成功");
//            producer.send(new ProducerRecord<>("xss","xss"+i));
//            使用同步发送消息  只有当收到ack之后才会发送下一条消息
            Future<RecordMetadata> fu = producer.send(new ProducerRecord<>("xss", "xss" + i));
//            其中阻塞就是使用的juc的get方法
            fu.get();
        System.out.println("======================");
        }
//        关闭资源
        producer.close();
    }
}
