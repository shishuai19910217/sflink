package com.sya.kafka.deserialization;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FastjsonKeyValueDeserializationSchema implements KafkaDeserializationSchema<JSONObject> {
    @Override
    public boolean isEndOfStream(JSONObject jsonObject) {
        return false;
    }

    @Override
    public JSONObject deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        byte[] value = consumerRecord.value();
        String msg = new String(value, "utf-8");
        JSONObject kafkaObj = JSONObject.parseObject(msg);
        JSONObject deviceDataPointDto = kafkaObj.getJSONObject("deviceDataPointDto");
        JSONObject parsedDataPointMessages = deviceDataPointDto.getJSONObject("parsedDataPointMessages");
        return parsedDataPointMessages;
    }

    @Override
    public TypeInformation<JSONObject> getProducedType() {
        return TypeExtractor.getForClass(JSONObject.class);
    }
}
