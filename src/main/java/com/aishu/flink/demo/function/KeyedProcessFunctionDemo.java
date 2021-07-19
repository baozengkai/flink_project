package com.aishu.flink.demo.function;


import com.aishu.flink.demo.model.CountWithTimestamp;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;


import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *  keyedProcessFunction场景测试
 *      场景说明:  监听本机9999端口，获取字符串
 *                 将每个字符串用空格间隔，得到Tuple实例，f0是分割后的单词， f1是1
 *                 利用f0分区得到keyedStream
 *                 自定义类继承KeyedProcessFunction: 实现记录每个单词最新一次出现的时间,然后创建一个十秒的定时器，十秒后若这个单词没有再出现，就把这个单词和总次数发送到下游算子
 */

public class KeyedProcessFunctionDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> socketStream = env.socketTextStream("localhost", 9999);

        // 分割字符串，并获得Tuple2<String, Long>类型
        DataStream<Tuple2<String, Long>> wordCount = socketStream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                String [] words = s.split(" ");
                for(String word :words){
                    collector.collect(new Tuple2<String, Long>(word, 1L));
                }
            }
        });

        // 继续处理成keyedStream类型并调用process进行细粒度处理
        // 所有输入的单词，如果超过10秒没有再出现，就在此打印出来
        wordCount.keyBy(0).process(new KeyedProcessFunction<Tuple, Tuple2<String, Long>, Tuple2<String, Long>>() {
            /**
             *  将每个单词最新出现时间记录到backend，并创建定时器
             *  定时器触发的时候，检查每个单词距离上次出现是否已经达到10秒，如果是，就发射给下游算子
             */
            // 自定义状态
            private ValueState<CountWithTimestamp> state;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 初始化状态
                state = getRuntimeContext().getState(new ValueStateDescriptor<CountWithTimestamp>("myState", CountWithTimestamp.class));
            }

            @Override
            public void processElement(Tuple2<String, Long> value, Context context, Collector<Tuple2<String, Long>> collector) throws Exception {
                // 取得当前是哪个单词
                Tuple currentKey = context.getCurrentKey();

                System.out.println(context.timestamp());
                // 从backend获取当前单词的state状态
                CountWithTimestamp currentState = state.value();

                // 如果状态还没有被初始化，那么在此进行初始化
                if(currentState == null){
                    currentState = new CountWithTimestamp();
                    currentState.key = value.f0;
                }

                // 单词数量加一
                currentState.count++;

                // 取当前时间戳， 作为该单词最后一次出现的时间
                currentState.lastModified = context.timerService().currentProcessingTime();

                // 重新保存到backend，包括该单词出现的次数，以及最后一次出现的时间
                state.update(currentState);

                // 为当前单词创建定时器，十秒后触发
                long timer = currentState.lastModified + 10000;

                context.timerService().registerProcessingTimeTimer(timer);

                // 打印所有信息，用于核对数据正确性
                System.out.println(String.format("process, %s, %d, lastModified : %d (%s), timer : %d (%s)\n\n",
                        currentKey.getField(0),
                        currentState.count,
                        currentState.lastModified,
                        time(currentState.lastModified),
                        timer,
                        time(timer)));
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext context, Collector<Tuple2<String, Long>> out) throws Exception {
                super.onTimer(timestamp, context, out);

                // 取得当前单词
                Tuple currentKey = context.getCurrentKey();

                // 获取单词的state状态
                CountWithTimestamp currentState = state.value();

                // 当前元素是否已经连续10秒未出现的标志
                boolean isTimeout = false;

                // timestamp是定时器触发的时间，如果等于最后一次更新时间+10秒，说明十秒内已经收到过该单词了
                // 这种连续10秒没有出现的元素，被发送到下游算子
                if(timestamp == currentState.lastModified +10000){
                    out.collect(new Tuple2<String, Long>(currentState.key, currentState.count));
                    isTimeout = true;
                }

                // 打印数据，用于核对是否符合预期
                System.out.println(String.format("ontimer, %s, %d, lastModified : %d (%s), stamp : %d (%s), isTimeout : %s\n\n",
                        currentKey.getField(0),
                        currentState.count,
                        currentState.lastModified,
                        time(currentState.lastModified),
                        timestamp,
                        time(timestamp),
                        String.valueOf(isTimeout)));
            }
        }).print();

        env.execute();
    }

    public static String time(long timeStamp) {
        return new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date(timeStamp));
    }

}
