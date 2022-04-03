package com.sya.kafka;

import com.sya.kafka.source.SourceBuilder;
import dto.SNStatusDto;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;


public class TumblingEventTimeWindowsJobnodata {
    public static void main(String[] args) throws Exception{
        // 获取 flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SourceBuilder sourceBuilder = new SourceBuilder();
       // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // 已经是默认的了

        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("localhost", 9999);
        env.setParallelism(1);
        SingleOutputStreamOperator<SNStatusDto> map = stringDataStreamSource.map(new SNStatusMapper());
// 提取事件时间
        WatermarkStrategy<SNStatusDto> wm = new WatermarkStrategy<SNStatusDto>() {
            @Override
            public WatermarkGenerator<SNStatusDto> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new MyBoundedOutOfOrdernessGenerator(0L);
            }
        }.withTimestampAssigner(new SerializableTimestampAssigner<SNStatusDto>() {
            @Override
            public long extractTimestamp(SNStatusDto element, long recordTimestamp) {
                return element.getTimestamp();
            }
        });
        map.assignTimestampsAndWatermarks((WatermarkStrategy<SNStatusDto>) new MyWatermarksWithIdleness<SNStatusDto>(new MyBoundedOutOfOrdernessGenerator(0), Duration.ofSeconds(1)));
        map.keyBy(new KeySelector<SNStatusDto, String>() {
            @Override
            public String getKey(SNStatusDto snStatusDto) throws Exception {
                return snStatusDto.getSn();
            }
        });

        env.execute();
    }

    /***
     * 将数据转成 SNStatusDto
     */
    static class SNStatusMapper implements MapFunction<String, SNStatusDto> {
        @Override
        public SNStatusDto map(String s) throws Exception {
            String[] split = s.split(",");
            String sn = split[0];
            String statusVal = split[1];
            String time = split[2];
            return new SNStatusDto(Long.parseLong(time),statusVal,sn);
        }
    }

    static  class MyBoundedOutOfOrdernessGenerator implements WatermarkGenerator<SNStatusDto> {

        private  long maxOutOfOrderness = 1000; // 1 秒

        private long currentMaxTimestamp;

        public MyBoundedOutOfOrdernessGenerator(long maxOutOfOrderness) {
            this.maxOutOfOrderness = maxOutOfOrderness;
        }

        @Override
        public void onEvent(SNStatusDto event, long eventTimestamp, WatermarkOutput output) {
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


    static class MyWatermarkStrategy implements WatermarkStrategy<SNStatusDto>{
        @Override
        public WatermarkGenerator<SNStatusDto> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new MyBoundedOutOfOrdernessGenerator(0L);
        }

        @Override
        public TimestampAssigner<SNStatusDto> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return WatermarkStrategy.super.createTimestampAssigner(context);
        }

        @Override
        public WatermarkStrategy<SNStatusDto> withTimestampAssigner(TimestampAssignerSupplier<SNStatusDto> timestampAssigner) {
            return WatermarkStrategy.super.withTimestampAssigner(timestampAssigner);
        }

        @Override
        public WatermarkStrategy<SNStatusDto> withTimestampAssigner(SerializableTimestampAssigner<SNStatusDto> timestampAssigner) {
            return WatermarkStrategy.super.withTimestampAssigner(timestampAssigner);
        }

        @Override
        public WatermarkStrategy<SNStatusDto> withIdleness(Duration idleTimeout) {
            return WatermarkStrategy.super.withIdleness(idleTimeout);
        }
    }
}
