package com.sya.kafka;

import com.sya.kafka.source.SourceBuilder;
import com.sya.dto.WInData;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class TumblingEventTimeWindowsJob1 {
    public static void main(String[] args) {
        // 获取 flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SourceBuilder sourceBuilder = new SourceBuilder();
       // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // 已经是默认的了
        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("localhost", 9999);
        env.setParallelism(1);
        // 提取事件时间
        WatermarkStrategy<WInData> wm = new WatermarkStrategy<WInData>() {
            @Override
            public WatermarkGenerator<WInData> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new MyBoundedOutOfOrdernessGenerator();
            }
        }.withTimestampAssigner(new SerializableTimestampAssigner<WInData>() {
            @Override
            public long extractTimestamp(WInData element, long recordTimestamp) {
                return element.getTime();
            }
        });

        KeyedStream<WInData, String> keyedStream = stringDataStreamSource.map(a -> {
            WInData wInData = new WInData();
            String[] split = a.split(",");
            String name = split[0];
            long s = Long.parseLong(split[1]);
            wInData.setTime(s);
            wInData.setName(name);
            return wInData;
        })
                .assignTimestampsAndWatermarks(wm.withIdleness(Duration.ofSeconds(1)))
                .keyBy(new KeySelector<WInData, String>() {
            @Override
            public String getKey(WInData wInData) throws Exception {
                return wInData.getName();
            }
        });
        keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new WindowFunction<WInData, WInData, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<WInData> input, Collector<WInData> out) throws Exception {
                        for (WInData wInData : input) {
                            wInData.setStart(window.getStart());
                            wInData.setEnd(window.getEnd());
                            out.collect(wInData);
                        }
                    }
                }).print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static  class MyBoundedOutOfOrdernessGenerator implements WatermarkGenerator<WInData> {

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
}
