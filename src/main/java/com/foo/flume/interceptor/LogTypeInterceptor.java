package com.foo.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.Map;

public class LogTypeInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //获取flume的body,body为json数组
        byte[] body = event.getBody();
        //将json数组转化为字符串
        String jsonstr = new String(body);
        String logType = "";
        //判断日志类型，start为启动日志，其他为事件日志
        if(jsonstr.contains("start")){
            logType = "start";
        }else{
            logType = "event";
        }

        //获取flume的消息头
        Map<String, String> headers = event.getHeaders();
        //将日志类型添加到flume消息头中
        headers.put("logType",logType);
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogTypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
