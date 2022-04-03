package com.sya.kafka.watermarks;

import dto.WInData;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

public class BoundedOutOfOrdernessGenerator12 implements WatermarkGenerator<WInData> {

    private final long maxOutOfOrderness = 1000; // 1 秒

    private long currentMaxTimestamp;

    @Override
    public void onEvent(WInData event, long eventTimestamp, WatermarkOutput output) {
        currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // 发出的 watermark = 当前最大时间戳 - 最大乱序时间

        long timestamp = currentMaxTimestamp - maxOutOfOrderness - 1;
        System.out.println("----可能发出的 watermark "+ timestamp + "--currentMaxTimestamp "+currentMaxTimestamp);
        output.emitWatermark(new Watermark(timestamp));
    }

}