package com.sya.kafka.map;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.api.java.tuple.Tuple3;

public class ToTuple implements MapFunction<JSONObject, Tuple3<String,Integer,Long>> {
    @Override
    public Tuple3<String, Integer, Long> map(JSONObject jsonObject) throws Exception {
        Tuple3<String, Integer, Long> tuple3 = new Tuple3<String, Integer, Long>();
        tuple3.setField(jsonObject.getInteger("age"), 1);
        tuple3.setField(jsonObject.getLong("time"), 2);
        tuple3.setField(jsonObject.getString("name"), 0);
        return tuple3;
    }
}
