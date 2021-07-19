package com.aishu.flink.demo.simple;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception{
        // 1.设置运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.配置数据源读取数据
        DataStreamSource<String> source = env.socketTextStream("localhost", 8888);

        DataStream<Tuple2<String, Integer>> resultStream = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String value, Collector<Tuple2<String, java.lang.Integer>> out) throws Exception {
                String[] words = value.split(",");
                for(String s : words)
                {
                    out.collect(new Tuple2<String, Integer>(s, 1));
                }
            }
        }).keyBy(0).sum(1);

        resultStream.print();

        env.execute();
    }
}
