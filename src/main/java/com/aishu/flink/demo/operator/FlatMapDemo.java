package com.aishu.flink.demo.operator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author: bao.zengkai
 * @Date: 2021/05/13
 */
public class FlatMapDemo {
    public static void main(String[] args) throws Exception{
        // 1.设置运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(10);

        // 2.读取数据源
        DataStreamSource<String> dataStreamSource = env.fromElements(new String[] {"hello, world", "nice"});

        DataStream<String> dataStream = dataStreamSource.flatMap(new FlatMapFunction<String, String>() {
            public void flatMap(String value, Collector<String> collector) throws Exception {
                String [] words = value.split("\\W+");
                for(String word : words){
                    collector.collect(word);
                }
            }
        }).rebalance();

        dataStream.print();
        env.execute();
    }
}
