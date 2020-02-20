package com.foo.flume.interceptor;


import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

//通过实现Interceptor拦截器接口
public class LogETLInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    //是一个事件
    @Override
    public Event intercept(Event event) {
        //获取传输过来的数据event.getBody()
        //此处body为原始数据，需要处理
        String body = new String(event.getBody(), Charset.forName("UTF-8"));
        //创建一个工具类创建一个方法进行校验数据
        if (LogUtils.vaildateReportLog(body)) {
            //通过校验就是我们的目标数据
            return event;
        }
//        没有通过校验就是我们的目标数据
        return null;
    }

    //是一个事件的集合
    @Override
    public List<Event> intercept(List<Event> events) {
        //创建一个list集合进行存储Event对象
        List<Event> list = new ArrayList<Event>(events.size());
        //遍历events集合
        for (Event event : events) {
            //调用上面intercept这个方法获取event值，将events集合中Event值进行过滤
            Event intercept = intercept(event);
            //判断intercept是否为空
            if(intercept!=null){
                //如果不为空将值添加到list集合中
                list.add(intercept);
            }
        }
        //最后返回这个集合
        return list;
    }

    @Override
    public void close() {

    }

    //内部类
    public static class Builder implements Interceptor.Builder{
        @Override
        public Interceptor build() {
            //创建LogETLInterceptor实例进行调用
            return new LogETLInterceptor();
        }

        //context上下文对象
        @Override
        public void configure(Context context) {

        }
    }
}
