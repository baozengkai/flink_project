package com.aishu.flink.demo.cep;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


/**
 *  测试模式定义-条件
 *      1.迭代条件
 *      2.组合条件
 *      3.中止条件
 */

public class PatternConditionDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> stream = env.fromElements(("a"), ("c"), ("b1"), ("b2"));

        // ------------------------------------------case 1-------------------------------------------------------------
        // 1.定义Pattern模式
        Pattern pattern = Pattern.<String>begin("start").where(new IterativeCondition<String>() {
            @Override
            public boolean filter(String s, Context<String> context) {
                return s.startsWith("a");
            }
        }).within(Time.seconds(10));

        // 2.模式检测
        PatternStream patternStream = CEP.pattern(stream, pattern);

        // 3.匹配事件提取
        patternStream.select(new PatternSelectFunction<String, String>() {
            //拿到每个pattern，保存的中间结果
            @Override
            public String select(Map<String, List<String>> pattern) throws Exception {
                return Arrays.toString(pattern.get("start").toArray());
            }
        }).print();

        // ------------------------------------------case 2-------------------------------------------------------------
        // 2.组合条件，通过.where().or()进行组合筛选
        Pattern pattern2 = Pattern.<String>begin("start").where(new IterativeCondition<String>() {
            @Override
            public boolean filter(String s, Context<String> context) {
                return s.startsWith("a");
            }
        }).or(new IterativeCondition<String>() {
            @Override
            public boolean filter(String s, Context<String> context) throws Exception {
                return s.startsWith("b");
            }
        }).within(Time.seconds(10));

        CEP.pattern(stream, pattern2).select(new PatternSelectFunction<String, String>() {
            //拿到每个pattern，保存的中间结果
            @Override
            public String select(Map<String, List<String>> pattern) throws Exception {
                return Arrays.toString(pattern.get("start").toArray());
            }

        }).print();

        env.execute();
    }
}
