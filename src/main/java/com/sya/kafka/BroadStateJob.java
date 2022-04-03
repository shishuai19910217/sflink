package com.sya.kafka;

import dto.WInData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BroadStateJob {
    public static void main(String[] args) {
        // 获取 flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("localhost", 9999);
        env.setParallelism(1);
        // 广播流 --规则
        DataStreamSource<String> rules = env.socketTextStream("localhost", 2222);
        // 为规则打标签
        MapStateDescriptor<String, String> stateDescriptor = new MapStateDescriptor<>("state", String.class, String.class);
        BroadcastStream<String> broadcast = rules.broadcast(stateDescriptor);

        BroadcastConnectedStream<String, String> connect = stringDataStreamSource.connect(broadcast);
        connect.process(new BroadcastProcessFunction<String, String, String>(){
            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {

            }

            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                // 把值放到广播流中
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(stateDescriptor);
                broadcastState.put("state", value);
            }
        });



        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    static class Mymapper implements MapFunction<String,WInData>, CheckpointedFunction {

        private transient ListState<Map<String,WInData>> checkpointedState  ;
        private Map<String,WInData> map = new HashMap<>(16);
        @Override
        public WInData map(String a) throws Exception {
            WInData wInData = new WInData();
            String[] split = a.split(",");
            String name = split[0];
            long s = Long.parseLong(split[1]);
            wInData.setTime(s);
            wInData.setName(name);
            map.put(name,wInData);
            System.out.println("---Mymapper---state--"+map.size());
            return wInData;
        }
        // Checkpoint触发时会调用这个方法，我们要实现具体的snapshot逻辑，比如将哪些本地状态持久化
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            checkpointedState.clear();
            // 保证
            checkpointedState.add(map);

        }
        // 第一次初始化时会调用这个方法，或者从之前的检查点恢复时也会调用这个方法
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            OperatorStateStore operatorStateStore = context.getOperatorStateStore();
            ListStateDescriptor<Map<String,WInData>> listStateDescriptor;
            ListStateDescriptor<Map<String,WInData>> descriptor =
                    new ListStateDescriptor<>(
                            "buffered-elements",
                            TypeInformation.of(new TypeHint<Map<String,WInData>>() {}));
            ListState<Map<String,WInData>> listState = operatorStateStore.getListState(descriptor);
            Iterable<Map<String, WInData>> maps = listState.get();
            for (Map<String, WInData> stringWInDataMap : maps) {
                this.map = stringWInDataMap;
                break;
            }

        }
    }
    static class Mymapper2 implements MapFunction<WInData,WInData>, CheckpointedFunction {

        private transient ListState<Map<String,WInData>> checkpointedState  ;
        private Map<String,WInData> map = new HashMap<>(16);
        @Override
        public WInData map(WInData a) throws Exception {
            map.put(a.getName(), a);
            System.out.println("---Mymapper2---state--"+map.size());
            return a;
        }
        // Checkpoint触发时会调用这个方法，我们要实现具体的snapshot逻辑，比如将哪些本地状态持久化
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            checkpointedState.clear();
            checkpointedState.add(map);

        }
        // 第一次初始化时会调用这个方法，或者从之前的检查点恢复时也会调用这个方法
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            OperatorStateStore operatorStateStore = context.getOperatorStateStore();
            ListStateDescriptor<Map<String,WInData>> listStateDescriptor;
            ListStateDescriptor<Map<String,WInData>> descriptor =
                    new ListStateDescriptor<>(
                            "buffered-elements",
                            TypeInformation.of(new TypeHint<Map<String,WInData>>() {}));
            ListState<Map<String,WInData>> listState = operatorStateStore.getListState(descriptor);
            Iterable<Map<String, WInData>> maps = listState.get();
            for (Map<String, WInData> stringWInDataMap : maps) {
                this.map = stringWInDataMap;
                break;
            }

        }
    }
}
