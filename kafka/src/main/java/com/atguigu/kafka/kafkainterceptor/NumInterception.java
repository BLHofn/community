package com.atguigu.kafka.kafkainterceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 自定义kafka拦截器：实现的需求是将时间戳信息加到消息value的最前部
 */
//这个方法就是在消息未进入到到分区和序列化时候调用
public class NumInterception implements ProducerInterceptor<String,String> {
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
//        得到还未进行任何处理的消息的value值
        String value = producerRecord.value();
//        给value值前面加上时间戳
        String myValue=value+ "," + System.currentTimeMillis();
//        将更改后的value值进行更新
        ProducerRecord<String, String> record = new ProducerRecord<>(producerRecord.topic(), producerRecord.partition(), producerRecord.key(), myValue);
        return record;
    }
//这个方法就是对消息是否发送成功和失败
    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
