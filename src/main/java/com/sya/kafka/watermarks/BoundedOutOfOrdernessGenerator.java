package com.sya.kafka.watermarks;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

public class BoundedOutOfOrdernessGenerator extends BoundedOutOfOrdernessTimestampExtractor<JSONObject> {


    public BoundedOutOfOrdernessGenerator(Time maxOutOfOrderness) {
        super(maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(JSONObject element) {
        return element.getTimestamp("time").getTime();
    }
}
