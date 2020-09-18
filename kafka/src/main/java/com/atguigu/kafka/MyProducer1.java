package com.atguigu.kafka;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
//带回调的生产者
public class MyProducer1 {
    public static void main(String[] args) {
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
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

//        创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
//        生产消息
        for (int i = 0; i < 10; i++) {
            int finalI = i;
//            使用回调函数  可以得到消息是否发送成功
//            如果不成功  他会重试不需要我们手动处理
            producer.send(new ProducerRecord<>("xss", "xss" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e!=null){
                        e.printStackTrace();
                    }else{
                        System.out.println("sucess");
                    }
                }
            });
        }
//        关闭资源
        producer.close();
    }
}
