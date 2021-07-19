package com.aishu.flink.demo.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class KafkaSourceDemoOne {
    public static void main(String[] args) throws Exception{
        // 1.设置运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.设置Kafka Source
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "aishu");

        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer011<String>("anyrobot_flink_demo",
                new SimpleStringSchema(), properties));

        // 3.打印Kafka信息
        stream.print();

        env.execute();
    }
}
