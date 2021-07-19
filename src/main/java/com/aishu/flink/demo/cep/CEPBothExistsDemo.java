package com.aishu.flink.demo.cep;

import com.aishu.flink.demo.model.LoginEvent;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CEPBothExistsDemo {
    public static void main(String[] args) throws Exception{
        //        // 1.初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.1 构建用户IP数据源
//        DataStream<LoginEvent> userIpEventDataStream = env.fromCollection(Arrays.asList(
//                new LoginEvent("101", "192.168.0.1", "UserIpEvent", "1"),
//                new LoginEvent("103", "192.168.0.3", "UserIpEvent", "1"),
//                new LoginEvent("102",  "192.168.0.2", "UserIpEvent", "2")
//        ));

//        DataStream<LoginEvent> userIpEventDataStream = env.fromCollection(Arrays.asList(
//                new LoginEvent("102",  "192.168.0.2", "UserIpEvent", "2"),
//                new LoginEvent("101", "192.168.0.1", "UserIpEvent", "1"),
//                new LoginEvent("103", "192.168.0.3", "UserIpEvent", "1")
//        ));

//        DataStream<LoginEvent> userIpEventDataStream = env.fromCollection(Arrays.asList(
//                new LoginEvent("102",  "192.168.0.2", "UserIpEvent", "2"),
//                new LoginEvent("104",  "192.168.0.4", "UserIpEvent", "4"),
//                new LoginEvent("101", "192.168.0.1", "UserIpEvent", "1"),
//                new LoginEvent("103", "192.168.0.3", "UserIpEvent", "1")
//
//        ));

        DataStream<LoginEvent> userIpEventDataStream = env.fromCollection(Arrays.asList(
                new LoginEvent("100", "192.168.0.0", "UserIpEvent", "1"),
                new LoginEvent("102",  "192.168.0.2", "UserIpEvent", "2"),
                new LoginEvent("104",  "192.168.0.4", "UserIpEvent", "4"),
                new LoginEvent("101", "192.168.0.1", "UserIpEvent", "1"),
                new LoginEvent("103", "192.168.0.3", "UserIpEvent", "1")

        ));
        AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipPastLastEvent();

        // 3.1 用户IP事件匹配规则
        Pattern<LoginEvent, ?> userIpEventPattern = Pattern.<LoginEvent>begin("begin", skipStrategy).
                where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent event, IterativeCondition.Context<LoginEvent> context) throws Exception {
//                        System.out.println("start: "+ event.toString());
                        return event.getRuleId().equals("1") || event.getRuleId().equals("2");
                    }
                })
                .followedBy("next")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent eventB, Context<LoginEvent> context) throws Exception {
//                        System.out.println("next: " + eventB.toString());
//                        return eventB.getRuleId().equals("2");
                        // 如果事件B跟事件A的Id不同，则匹配
                        if (eventB.getType().equals("UserIpEvent") && (eventB.getRuleId().equals("1") || eventB.getRuleId().equals("2")))
                        {
                            Iterable<LoginEvent> personEvents = context.getEventsForPattern("begin");


                            while(personEvents.iterator().hasNext())
                            {
                                LoginEvent eventA = personEvents.iterator().next();

                                if (eventB.getRuleId().equals(eventA.getRuleId()))
                                {
                                    return false;
                                }
                                return true;
                            }
                        }
                        return false;
                    }
                }).within(Time.seconds(10));


        PatternStream<LoginEvent> userIpEventPatternStream = CEP.pattern(userIpEventDataStream, userIpEventPattern);

        DataStream<String> userIpPatternDS =  userIpEventPatternStream.select((Map<String, List<LoginEvent>> pattern) -> {
            List<LoginEvent> first = pattern.get("begin");
            List<LoginEvent> second = pattern.get("next");

            System.out.println("And告警场景-第一个元素: " + first.toString());
            System.out.println("And告警场景-第二个元素: " + second.toString());

            return second.toString();
        });
        env.execute();
    }
}
