//package com.sya.kafka;
//
//import com.alibaba.fastjson.JSONObject;
//import com.sya.kafka.keyby.SnKeyBy;
//import com.sya.kafka.map.ToTuple;
//import com.sya.kafka.source.SourceBuilder;
//import com.sya.kafka.watermarks.BoundedOutOfOrdernessGenerator;
//import org.apache.flink.api.common.functions.IterationRuntimeContext;
//import org.apache.flink.api.common.functions.RuntimeContext;
//import org.apache.flink.api.java.functions.KeySelector;
//import org.apache.flink.api.java.tuple.Tuple;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.tuple.Tuple3;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.datastream.*;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
//import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
//import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
//import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
//import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
//import org.apache.flink.util.Collector;
//import org.apache.flink.util.TimeUtils;
//
//public class KafKaJob {
//    public static void main(String[] args) {
//        // 获取 flink 执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        SourceBuilder sourceBuilder = new SourceBuilder();
//       // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // 已经是默认的了
//        DataStream<JSONObject> dataStreamSource = env.addSource(sourceBuilder.getMySensorSource());
//        dataStreamSource = dataStreamSource
//                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator(Time.seconds(0)));
//
//        KeyedStream<JSONObject, String> keybyDataStream = dataStreamSource.keyBy(new SnKeyBy());
//        WindowedStream<JSONObject, String, TimeWindow> window =
//                keybyDataStream.window(TumblingEventTimeWindows.of(Time.seconds(2)));
//        window
//                //.allowedLateness(Time.seconds(10))
//                .apply(new WindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
//            @Override
//            public void apply(String s, TimeWindow window, Iterable<JSONObject> input, Collector<JSONObject> out) throws Exception {
//                JSONObject val = null;
//                long start = window.getStart();
//
//                long end = window.getEnd();
//
//                for (JSONObject object : input) {
//                    val = object;
//                    val.put("window","-----start:--"+start+"----end:--"+end);
//                    out.collect(val);
//                }
//
//            }
//        })
//               .print();
//        try {
//            env.execute();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//}
