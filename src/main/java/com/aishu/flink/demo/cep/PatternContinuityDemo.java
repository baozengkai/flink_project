package com.aishu.flink.demo.cep;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 *  测试模式定义-组合模式
 *      1.严格连续性
 *      2.宽松连续性
 *      3.非确定性宽松连续性
 */

public class PatternContinuityDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> dataStream = env.fromElements(("a"), ("c"));

        /*---------严格连续模式----------------------*/
        Pattern strictPattern = Pattern.begin("start").where(new IterativeCondition<Object>() {
            @Override
            public boolean filter(Object s, Context<Object> context) {
                System.out.println(s.toString());
                return s.toString().equalsIgnoreCase("a");
            }
        }).next("middle").where(new IterativeCondition<Object>() {
            @Override
            public boolean filter(Object o, Context<Object> context) {
                System.out.println(o.toString());
                return o.toString().contains("c");
            }
        });

        CEP.pattern(dataStream, strictPattern).select(new PatternSelectFunction<String, String>() {
            @Override
            public String select(Map<String, List<String>> map) throws Exception {
                List<String> relaxed_start = map.get("start");
                List<String> relaxed_middle = map.get("middle");
                return relaxed_start.toString() + ", " + relaxed_middle.toString();
            }
        }).print();


        /*---------松散连续----------------------*/
//        Pattern relaxedPattern = Pattern.begin("start").where(new IterativeCondition<Object>() {
//            @Override
//            public boolean filter(Object s, Context<Object> context) {
//                return s.toString().equalsIgnoreCase("a");
//            }
//        }).followedBy("middle").where(new IterativeCondition<Object>() {
//            @Override
//            public boolean filter(Object o, Context<Object> context) {
//                return o.toString().contains("b");
//            }
//        });
//
//        CEP.pattern(dataStream, relaxedPattern).select(new PatternSelectFunction<String, String>() {
//            @Override
//            public String select(Map<String, List<String>> map) throws Exception {
//                List<String> relaxed_start = map.get("start");
//                List<String> relaxed_middle = map.get("middle");
//                return relaxed_start.toString() + ", " + relaxed_middle.toString();
//            }
//        }).print();
//
//        /*---------不确定的松散连续----------------------*/
//        Pattern nonDeterminPattern = Pattern.begin("start").where(new IterativeCondition<Object>() {
//            @Override
//            public boolean filter(Object s, Context<Object> context) {
//                return s.toString().equalsIgnoreCase("a");
//            }
//        }).followedByAny("middle").where(new IterativeCondition<Object>() {
//            @Override
//            public boolean filter(Object o, Context<Object> context) {
//                return o.toString().contains("b");
//            }
//        });
//
//        CEP.pattern(dataStream, nonDeterminPattern).select(new PatternSelectFunction<String, String>() {
//            @Override
//            public String select(Map<String, List<String>> map) throws Exception {
//                List<String> nonDeterminStart = map.get("start");
//                List<String> nonDeterminMiddle = map.get("middle");
//                return nonDeterminStart.toString() + ", " + nonDeterminMiddle.toString();
//            }
//        }).print();
        /*---------------------------------------------*/
        env.execute();
    }
}
