package com.sya.kafka.datapointalarm.rule.source;

import com.alibaba.fastjson.JSONObject;
import com.sya.config.PropertiesUnit;
import com.sya.kafka.deserialization.FastjsonKeyValueDeserializationSchema;
import com.sya.kafka.source.MySensorSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;


public class SourceBuilder {
    private static final Properties kafkaProperties = PropertiesUnit.getProperties("kafka.properties");

    /***
     * 数据点上报的数据
     * @return
     */
    public static FlinkKafkaConsumer getKafkaPointDataSource(){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaProperties.getProperty("bootstrap.servers"));
        properties.setProperty("group.id", kafkaProperties.getProperty("pointdata.group.id"));
        FlinkKafkaConsumer<JSONObject> source =
                new FlinkKafkaConsumer<JSONObject>(kafkaProperties.getProperty("pointdata.topic"),new FastjsonKeyValueDeserializationSchema(),properties);
        return source;

    }

    /***
     * 规则动态修改
     * @return
     */
    public static FlinkKafkaConsumer getKafkaRuleModifySource(){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaProperties.getProperty("bootstrap.servers"));
        properties.setProperty("group.id", kafkaProperties.getProperty("rule.modify.group.id"));
        FlinkKafkaConsumer<JSONObject> source =
                new FlinkKafkaConsumer<JSONObject>(kafkaProperties.getProperty("rule.modify.topic"),new FastjsonKeyValueDeserializationSchema(),properties);
        return source;

    }

    /***
     * 规则动态修改
     * @return
     */
    public static FlinkKafkaConsumer getKafkaMachineMapperModifySource(){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaProperties.getProperty("bootstrap.servers"));
        properties.setProperty("group.id", kafkaProperties.getProperty("rule.modify.group.id"));
        FlinkKafkaConsumer<JSONObject> source =
                new FlinkKafkaConsumer<JSONObject>(kafkaProperties.getProperty("rule.modify.topic"),new FastjsonKeyValueDeserializationSchema(),properties);
        return source;

    }
    /***
     * 规则动态修改
     * @return
     */
    public static FlinkKafkaConsumer getKafkaDeviceUniqueDataPointIdMapperModifySource(){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaProperties.getProperty("bootstrap.servers"));
        properties.setProperty("group.id", kafkaProperties.getProperty("deviceUniqueDataPointId.modify.group.id"));
        FlinkKafkaConsumer<JSONObject> source =
                new FlinkKafkaConsumer<JSONObject>(kafkaProperties.getProperty("deviceUniqueDataPointId.modify.topic"),new FastjsonKeyValueDeserializationSchema(),properties);
        return source;

    }

}
