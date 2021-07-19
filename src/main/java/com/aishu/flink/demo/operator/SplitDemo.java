package com.aishu.flink.demo.operator;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class SplitDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> dataStreamSource = env.generateSequence(0, 10);

        SplitStream<Long> splitStream = dataStreamSource.split(new OutputSelector<Long>() {
            public Iterable<String> select(Long value) {
                List<String> out = new ArrayList<String>();
                if (value % 2 == 0){
                    out.add("even");
                }else{
                    out.add("odd");
                }
                return out;
            }
        });

        splitStream.select("even").print();
//        splitStream.select("even", "odd").print();

        env.execute();
    }
}
