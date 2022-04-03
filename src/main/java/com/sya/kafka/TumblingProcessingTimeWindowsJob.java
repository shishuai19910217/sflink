package com.sya.kafka;

import com.sya.kafka.source.SourceBuilder;
import com.sya.kafka.watermarks.BoundedOutOfOrdernessGenerator12;
import dto.WInData;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class TumblingProcessingTimeWindowsJob {
    public static void main(String[] args) {
        // 获取 flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SourceBuilder sourceBuilder = new SourceBuilder();
        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("localhost", 9999);
        env.setParallelism(1);

        KeyedStream<WInData, String> keyedStream = stringDataStreamSource.map(a -> {
            WInData wInData = new WInData();
            String[] split = a.split(",");
            String name = split[0];
            long s = Long.parseLong(split[1]);
            wInData.setTime(s);
            wInData.setName(name);
            return wInData;
        })

                .keyBy(new KeySelector<WInData, String>() {
            @Override
            public String getKey(WInData wInData) throws Exception {
                return wInData.getName();
            }
        });
        keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
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
}
