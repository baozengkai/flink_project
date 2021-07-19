package com.aishu.flink.demo.state;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.util.Collector;
import cn.hutool.core.date.DateTime;
import java.util.Date;


public class DetectionScenarioDemoOne {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> socketStream = env.socketTextStream("localhost", 7777);

        // 分割字符串，并获得Tuple2<String, Long>类型
        DataStream<JSONObject> jsonEvent = socketStream.flatMap(new FlatMapFunction<String, JSONObject>() {
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                collector.collect(JSONObject.parseObject(s));
            }
        });

        jsonEvent.keyBy(new KeySelector<JSONObject, Object>() {
            @Override
            public Object getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.get("id");
            }
        }).process(new KeyedProcessFunction<Object, JSONObject, String>() {
            private static final long serialVersionUID = 1L;

            private static final double SMALL_AMOUNT = 1.00;
            private static final double LARGE_AMOUNT = 500.00;
            private static final long ONE_MINUTE = 10 * 1000;

            private transient ValueState<Boolean> flagState;
            private transient ValueState<Long> timerState;

            @Override
            public void open(Configuration parameters) {
                ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
                        "flag",
                        Types.BOOLEAN);
                flagState = getRuntimeContext().getState(flagDescriptor);

                ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>(
                        "timer-state",
                        Types.LONG);
                timerState = getRuntimeContext().getState(timerDescriptor);
            }

            @Override
            public void processElement(JSONObject transaction, Context context, Collector<String> collector) throws Exception {
                // Get the current state for the current key
                Boolean lastTransactionWasSmall = flagState.value();
                System.out.println(lastTransactionWasSmall);

                System.out.println(timerState.value());

                // Check if the flag is set
                if (lastTransactionWasSmall != null) {
                    System.out.println("要产生告警");
                    if (transaction.getIntValue("amount") > LARGE_AMOUNT) {
                        //Output an alert downstream
//                Alert alert = new Alert();
//                alert.setId(transaction.getAccountId());

                        collector.collect("产生告警");
                    }
                    // Clean up our state
                    cleanUp(context);
                }

                if (transaction.getIntValue("amount") < SMALL_AMOUNT) {
                    System.out.println("我进来了");
                    // set the flag to true
                    flagState.update(true);
                    System.out.println("进来之后更新的状态:" + flagState.value());

                    long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
                    context.timerService().registerProcessingTimeTimer(timer);

                    timerState.update(timer);
                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) {
                // remove flag after 1 minute
                timerState.clear();
                flagState.clear();
                System.out.println("执行定时清理操作, 现在时间为: " + DateTime.now());
            }

            private void cleanUp(Context ctx) throws Exception {
                // delete timer
                Long timer = timerState.value();
                ctx.timerService().deleteProcessingTimeTimer(timer);

                // clean up all state
                timerState.clear();
                flagState.clear();
            }
        });

        env.execute();

    }
}


