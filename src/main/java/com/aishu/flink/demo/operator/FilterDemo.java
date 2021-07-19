package com.aishu.flink.demo.operator;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterDemo {
    public static void main(String[] args) throws Exception{
        // 1.设置运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.读取数据源
        DataStreamSource<Long> dataStreamSource = env.generateSequence(0, 10);

        DataStream<Long> filterDataSource = dataStreamSource.filter(new FilterFunction<Long>() {
            public boolean filter(Long aLong) throws Exception {
                if (aLong % 2 ==0) {
                    return false;
                }else{
                    return true;
                }
            }
        });
        filterDataSource.print();

        env.execute();
    }
}
