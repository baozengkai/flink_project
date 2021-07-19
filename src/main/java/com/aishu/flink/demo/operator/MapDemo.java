package com.aishu.flink.demo.operator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: bao.zengkai
 * @Date: 2021/05/13
 */
public class MapDemo {
    public static void main(String[] args) throws Exception{
        // 1.设置运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.读取数据源
        DataStreamSource<Long> dataStreamSource = env.generateSequence(0, 10);

//        dataStreamSource.print();

        DataStream<Long> plusCounter = dataStreamSource.map(new MapFunction<Long, Long>() {
            public Long map(Long value) throws Exception {
                System.out.println("------------------" + value);
                return value + 1;
            }
        });
        plusCounter.print();

        env.execute();
    }
}
