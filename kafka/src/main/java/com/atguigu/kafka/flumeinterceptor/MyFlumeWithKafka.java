package com.atguigu.kafka.flumeinterceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.Map;

/**
 * 将日志数据中带有atguigu的，输入到Kafka的first主题中，
 * 将日志数据中带有shangguigu的,输入到Kafka的second主题中，
 * 其他的数据输入到Kafka的third主题中
 */
public class MyFlumeWithKafka implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        String str = new String(event.getBody());
        Map<String, String> headers = event.getHeaders();
        if (str.contains("atguigu")){
            headers.put("topic","first");
        }else if (str.contains("ssg")){
//            必须使用topic  a1.sinks.k1.kafka.topic配置文件规定
            headers.put("topic","second");
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            intercept(event);
        }
        return null;
    }

    @Override
    public void close() {

    }
    public static class MyBuilder implements org.apache.flume.interceptor.Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new MyFlumeWithKafka();
        }

        @Override
        public void configure(Context context) {
        }
    }
}
