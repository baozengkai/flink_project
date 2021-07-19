package com.aishu.flink.demo.state;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 *  测试广播流
 *      场景1: 2条流进行connect结合，结合后的流调用process实现BroadcastProcessFunction方法进行代码逻辑变更
 */

public class BroadCastDemo {
    public static void main(String[] args) throws Exception{
        //-----------------------------------case 1---------------------------------------------------------------------
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputDataStream = env.socketTextStream("localhost", 8888);
        DataStreamSource<String> controlDataStream = env.socketTextStream("localhost", 9999);

        // 1.定义一个描述符，控制流
        MapStateDescriptor<String, String> brocastDescriptor = new MapStateDescriptor<String, String>("brocast-state", Types.STRING, Types.STRING);
        BroadcastStream<String> controlBroadCastStrean = controlDataStream.broadcast(brocastDescriptor);

        // 2.把数据流和广播流结合起来
        BroadcastConnectedStream<String, String> inputBroadCastConnectedStream = inputDataStream.connect(controlBroadCastStrean);

        // 3.调用process实现BrocastProcessFunction中的processElement方法和processBroadcastElement方法
        inputBroadCastConnectedStream.process(new BroadcastProcessFunction<String, String, Object>() {
            @Override
            public void processElement(String s, ReadOnlyContext readOnlyContext, Collector<Object> collector) throws Exception {
                System.out.println("我是数据流...");
                ReadOnlyBroadcastState<String, String> broadcastState = readOnlyContext.getBroadcastState(brocastDescriptor);
                String switchValue = broadcastState.get("Switch");
                if("1".equals(switchValue)){
                    collector.collect("切换到1的逻辑");
                }else if("2".equals(switchValue)){
                    collector.collect("切换到2的逻辑");
                }
            }

            @Override
            public void processBroadcastElement(String s, Context context, Collector<Object> collector) throws Exception {
                System.out.println("我是广播流...");
                BroadcastState<String, String> broadcastState = context.getBroadcastState(brocastDescriptor);
                broadcastState.put("Switch", s);
            }
        }).print();

        env.execute();
    }
}
