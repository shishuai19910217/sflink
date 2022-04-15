package com.sya.kafka.datapointalarm.rule.link;

import lombok.Data;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.io.Serializable;

@Data
public class LinkBuilder implements Serializable {

    public static void kafkaLink(){
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<String>(
                "localhost:9092", "flink-topic", new SimpleStringSchema()
        );
    }
}
