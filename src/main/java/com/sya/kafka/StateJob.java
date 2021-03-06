package com.sya.kafka;

import com.sya.dto.WInData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;

public class StateJob {
    public static void main(String[] args) {
        // 获取 flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("localhost", 9999);
        env.setParallelism(1);
        stringDataStreamSource.map(new Mymapper()).print();




        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    static class Mymapper extends RichMapFunction<String, WInData> implements CheckpointedFunction {

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
