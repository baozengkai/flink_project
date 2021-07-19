package com.aishu.flink.demo.operator;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.ArrayList;
import java.util.List;


public class ProjectDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Tuple4<String, String, String, Integer>> tuples = new ArrayList<Tuple4<String, String, String, Integer>>();
        tuples.add(new Tuple4<String, String, String, Integer>("class3", "zhangsan", "语文", 100));
        tuples.add(new Tuple4<String, String, String, Integer>("class4", "zhaoliu", "语文", 81));

        DataStreamSource<Tuple4<String, String, String, Integer>> dataStreamSource = env.fromCollection(tuples);

        dataStreamSource.project(0,1).print();

        env.execute();
    }
}
