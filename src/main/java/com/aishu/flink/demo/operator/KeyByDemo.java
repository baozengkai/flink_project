package com.aishu.flink.demo.operator;

import org.apache.flink.api.java.Utils.*;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class KeyByDemo {
    public static void main(String[] args) throws Exception{
        // 1.设置运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Tuple4<String, String, String, Integer>> tuples = new ArrayList<Tuple4<String, String, String, Integer>>();
        tuples.add(new Tuple4<String, String, String, Integer>("class1", "zhangsan", "语文", 100));
        tuples.add(new Tuple4<String, String, String, Integer>("class1", "lisi", "语文", 78));
        tuples.add(new Tuple4<String, String, String, Integer>("class1", "wangwu", "语文", 99));
        tuples.add(new Tuple4<String, String, String, Integer>("class2", "zhaoliu", "语文", 81));
        tuples.add(new Tuple4<String, String, String, Integer>("class2", "qianqi", "语文", 59));
        tuples.add(new Tuple4<String, String, String, Integer>("class2", "maba", "语文", 97));


        DataStreamSource<Tuple4<String, String, String, Integer>> dataStreamSource = env.fromCollection(tuples);

        KeyedStream keyedStream = dataStreamSource.keyBy(0);
        keyedStream.max(3).print();

        env.execute();

    }

}
