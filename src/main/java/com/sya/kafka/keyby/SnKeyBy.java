package com.sya.kafka.keyby;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.functions.KeySelector;

public class SnKeyBy implements KeySelector<JSONObject,String> {
    @Override
    public String getKey(JSONObject jsonObject) throws Exception {

        return jsonObject.getString("name");
    }
}
