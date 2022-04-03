//package com.sya.kafka.source;
//
//import com.alibaba.fastjson.JSONObject;
//import org.apache.flink.api.common.state.BroadcastState;
//import org.apache.flink.api.common.state.MapStateDescriptor;
//import org.apache.flink.runtime.state.FunctionInitializationContext;
//import org.apache.flink.runtime.state.FunctionSnapshotContext;
//import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
//import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
//
///***
// * 定义一个时钟
// */
//public class MyRuleSource extends RichParallelSourceFunction<AlarmRuleDto> implements CheckpointedFunction {
//    private MapStateDescriptor<String, JSONObject> stateDescriptor =
//            new MapStateDescriptor("snTemplateDatapoint", String.class, JSONObject.class);
//
//    @Override
//    public void run(SourceContext<AlarmRuleDto> ctx) throws Exception {
//       int i = 0;
//        while (true) {
//           Thread.sleep(1000);
//            AlarmRuleDto alarmRuleDto = new AlarmRuleDto();
//            alarmRuleDto.setSn("asd");
//            JSONObject a = new JSONObject();
//            a.put("offTime",5000);
//            a.put("offCount",5);
//            alarmRuleDto.setData(a);
//            ctx.collect(alarmRuleDto);
//        }
//    }
//
//    @Override
//    public void cancel() {
//
//    }
//
//    @Override
//    public void snapshotState(FunctionSnapshotContext context) throws Exception {
//
//    }
//
//    @Override
//    public void initializeState(FunctionInitializationContext context) throws Exception {
//        // 为规则打标签
//
//        BroadcastState<String, JSONObject> broadcastState = context.getOperatorStateStore().getBroadcastState(stateDescriptor);
//        // 初始化数据
//        JSONObject a = new JSONObject();
//        a.put("offTime",5000);
//        a.put("offCount",5);
//        broadcastState.put("a",a);
//
//        JSONObject b = new JSONObject();
//        b.put("offTime",5000);
//        b.put("offCount",5);
//        broadcastState.put("b",b);
//    }
//}
