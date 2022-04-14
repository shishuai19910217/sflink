package com.sya.kafka.source;

import com.alibaba.fastjson.JSONObject;
import com.sya.kafka.deserialization.FastjsonKeyValueDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.util.Properties;


public class SourceBuilder {

    public FlinkKafkaConsumer getKafkaSource(){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "139.196.142.241:9092");
        properties.setProperty("group.id", "test-flink-local");
        FlinkKafkaConsumer<JSONObject> source =
                new FlinkKafkaConsumer<JSONObject>("test-ss",new FastjsonKeyValueDeserializationSchema(),properties);
        return source;

    }
    public MySensorSource getMySensorSource(){
        return new MySensorSource();
    }

}
