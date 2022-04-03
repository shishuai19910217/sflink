package com.sya.kafka.watermarks;

import org.apache.flink.api.common.eventtime.*;

import java.time.Duration;

public class WatermarkStrategya implements WatermarkStrategy {
    @Override
    public WatermarkGenerator createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return null;
    }

    @Override
    public TimestampAssigner createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return WatermarkStrategy.super.createTimestampAssigner(context);
    }

    @Override
    public WatermarkStrategy withTimestampAssigner(TimestampAssignerSupplier timestampAssigner) {
        return WatermarkStrategy.super.withTimestampAssigner(timestampAssigner);
    }

    @Override
    public WatermarkStrategy withTimestampAssigner(SerializableTimestampAssigner timestampAssigner) {
        return WatermarkStrategy.super.withTimestampAssigner(timestampAssigner);
    }

    @Override
    public WatermarkStrategy withIdleness(Duration idleTimeout) {
        return WatermarkStrategy.super.withIdleness(idleTimeout);
    }
}
