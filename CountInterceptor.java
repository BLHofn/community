package com.atguigu.kafka.kafkainterceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 自定义kafka的inteor:实现对消息的成功和失败进行计数
 */
public class CountInterceptor implements ProducerInterceptor<String,String> {
//    定义我们成功的次数和失败的次数
    private int sucessCount=0;
    private int faildCount=0;
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        return producerRecord;
    }
//对消息是否发送成功和失败进行计数
    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (e!=null){
            faildCount++;
        }else{
            sucessCount++;
        }
    }
//在关闭资源的时候打印我们的消息成功数和失败数
    @Override
    public void close() {
        System.out.println("sucess: "+sucessCount);
        System.out.println("faild: "+faildCount);
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
