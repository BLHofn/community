package com.atguigu.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 自动提交offert：
 * 重置offset：前提条件  就是这个消费者是刚加入的从未消费过或者是就是在kafka中消费过但是他的记录已经超过七天被删除了
 *              默认的是Default:	latest（最新的消息）
 *              earliest：是最早的（既是所有的消息）
 * 手动提交offset（不常用）：
 */
public class MyConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop101:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ssg1");
//        设置自动提交offset的功能
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
//        设置offset自动提交的时间间隔
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
//        设置哪些重置的offset参数
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
//        对key和value进行反序列化
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
//        创建消费者
        KafkaConsumer<String,String> consumer=new KafkaConsumer<>(props);
//        发布消息
        List<String> list=new ArrayList<>();
        list.add("first");
        list.add("second");
        list.add("xss");
        consumer.subscribe(list);
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
//                得到主题
                String topic = record.topic();
//                得到值
                String value = record.value();
//                得到分区
                int partition = record.partition();
//                得到offset
                long offset = record.offset();
                System.out.println("topic = "+topic+" value = "+value+" partition = "+partition+" offset = " + offset);
            }
        }
    }
}
