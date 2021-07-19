package com.aishu.flink.demo.operator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

public class ConnectedDemo {
    public static void main(String[] args) throws Exception{
        // 1.设置运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 2.读取数据源
        DataStreamSource<String> dataStreamSourceOne = env.fromElements(new String[] {"hello, world", "nice"});
        DataStreamSource<Long> dataStreamSourceTwo = env.generateSequence(0, 10);
        
        ConnectedStreams<String , Long> connectedStream= dataStreamSourceOne.connect(dataStreamSourceTwo);
        
        connectedStream.flatMap(new CoFlatMapFunction<String, Long, Object>() {
            public void flatMap1(String input1, Collector<Object> out) throws Exception {
                String[] words = input1.split(",");
                for (String word: words)
                {
                    out.collect(word);
                }
            }

            public void flatMap2(Long input2, Collector<Object> out) throws Exception {
                out.collect(input2.toString());
            }
        }).print();

        connectedStream.map(new CoMapFunction<String, Long, Object>() {
            public Object map1(String s) throws Exception {
                return s;
            }

            public Object map2(Long aLong) throws Exception {
                return aLong;
            }
        }).print();
        env.execute();
    }
}
