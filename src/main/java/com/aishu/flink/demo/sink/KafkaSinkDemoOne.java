package com.aishu.flink.demo.sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.FlatMapIterator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Properties;


public class KafkaSinkDemoOne {
    public static void main(String[] args) throws Exception{
        // 1.设置运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.配置数据源读取数据
        DataStreamSource<String> source = env.fromElements("a,b", "c,d,e", "f,g");

        DataStream<String> dataStream = source.flatMap(new FlatMapFunction<String, String>() {
            public void flatMap(String s, Collector<String> collector) throws Exception {
                for(String key: s.split(",")){
                    collector.collect(key);
                }
            }
        });

        FlinkKafkaProducer011<String> kafkaProducer011 = new FlinkKafkaProducer011<String>("localhost:9092",
                "anyrobot_flink_demo", new SimpleStringSchema());

        dataStream.addSink(kafkaProducer011);

        env.execute();
    }
}
