package com.foo.flume.interceptor;

import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogUtils {
    //log日志对象
    private static Logger logger = LoggerFactory.getLogger(LogUtils.class);

    public static boolean vaildateReportLog(String log) {
        /*
             日志检查，正常的返回true，错误的返回false
         */
        try {
            //校验总长度
            if (log.split("\\|").length < 2) {
                return false;
            }
            //其次校验的是第一串是否为时间戳
            else if (log.split("\\|")[0].length() != 13 || !NumberUtils.isNumber(log.split("\\|")[0])) {
                return false;
            }
            //再次判断第二部分是否为正确的json字符串
            //trim截断
            else if (!log.split("\\|")[1].trim().startsWith("{") || !log.split("\\|")[1].trim().endsWith("}")) {
                return false;
            }
        }catch (Exception e){
            //错误打印的日志
            logger.error("error parse,message is:"+log);
            //将抓到的日志进行输出
            logger.error(e.getMessage());
            return false;
        }
        return true;
    }
}
