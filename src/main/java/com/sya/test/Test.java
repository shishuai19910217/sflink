package com.sya.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        MapStateDescriptor<String, String> stateDescriptor
                = new MapStateDescriptor<>("state", String.class, String.class);
        DataStreamSource<String> dataStreamSource = env.socketTextStream("127.0.0.1", 9999);
        DataStreamSource<String> rules = env.socketTextStream("localhost", 2222);
        rules.setParallelism(1);
        BroadcastStream<String> broadcast = rules.broadcast(stateDescriptor);
        DataStream<Dto> map = dataStreamSource.map(new MapFunction<String, Dto>() {
            @Override
            public Dto map(String s) throws Exception {
                String[] split = s.split(",");
                Dto dto = new Dto();
                dto.setSn(split[0]);
                dto.setVal(split[1]);
                return dto;
            }
        });
        KeyedStream<Dto, String> dtoStringKeyedStream = map.keyBy(new KeySelector<Dto, String>() {
            @Override
            public String getKey(Dto dto) throws Exception {
                return dto.getSn();
            }
        });

        BroadcastConnectedStream<Dto, String> connect = dtoStringKeyedStream.connect(broadcast);
        DataStream<Dto> process = connect.process(new KeyedBroadcastProcessFunction<String, Dto, String, Dto>() {
            @Override
            public void processElement(Dto value, ReadOnlyContext ctx, Collector<Dto> out) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor =
                        new ValueStateDescriptor(value.getSn(), String.class);
                ValueState<String> state = getRuntimeContext().getState(valueStateDescriptor);
                String value1 = state.value();
                System.out.println("------------aa---sn-"+value.getSn());
                System.out.println("------------aa---state-"+state.value());
                ReadOnlyBroadcastState<String, String> aaa = ctx.getBroadcastState(stateDescriptor);
                String s = aaa.get(value.getSn());
                System.out.println("-----ssssss"+s);
                aaa.clear();

                out.collect(value);
            }

            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<Dto> out) throws Exception {
                String[] split = value.split(",");
                String sn = split[0];
                String s = split[1];
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(stateDescriptor);
                broadcastState.put(sn,"1");

            }


        });



        env.execute();

    }
}
