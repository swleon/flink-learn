package com.hb.flink.java.course05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * DataStream
 */
public class DataStreamSourceJavaApp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        socketFunction(env);

        env.execute("DataStreamSourceJavaApp");
    }

    /**
     * 从socket创建datastream
     * @param env
     */
    private static void socketFunction(StreamExecutionEnvironment env) {

        DataStreamSource<String> data = env.socketTextStream("localhost",9999);
        data.print().setParallelism(1);
    }
}
