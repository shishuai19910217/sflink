//package com.sya.kafka;
//
//import com.alibaba.fastjson.JSONObject;
//import com.sya.kafka.source.MyRuleSource;
//import dto.SNStatusDto;
//import org.apache.flink.api.common.functions.*;
//import org.apache.flink.api.common.state.*;
//import org.apache.flink.api.java.functions.KeySelector;
//import org.apache.flink.runtime.state.FunctionInitializationContext;
//import org.apache.flink.runtime.state.FunctionSnapshotContext;
//import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
//import org.apache.flink.streaming.api.datastream.*;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
//import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
//import org.apache.flink.util.Collector;
//
//import java.util.List;
//
//public class BroadStateJobRule {
//    public static void main(String[] args) {
//        // 获取 flink 执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("localhost", 9999);
//        env.setParallelism(1);
//        DataStream<SNStatusDto> map = stringDataStreamSource.map(new SNStatusMapper());
//        KeyedStream<SNStatusDto, String> snStatusDtoStringKeyedStream = map.keyBy(new KeySelector<SNStatusDto, String>() {
//            @Override
//            public String getKey(SNStatusDto snStatusDto) throws Exception {
//                return snStatusDto.getSn();
//            }
//        });
//        // 广播流 --规则
//        DataStreamSource<AlarmRuleDto> rules = env.addSource(new MyRuleSource());
//        // 为规则打标签
//        MapStateDescriptor<String, JSONObject> stateDescriptor =
//                new MapStateDescriptor("snTemplateDatapoint", String.class, JSONObject.class);
//
//        BroadcastStream<AlarmRuleDto> broadcast = rules.broadcast(stateDescriptor);
//        BroadcastConnectedStream<SNStatusDto, AlarmRuleDto> connect = snStatusDtoStringKeyedStream.connect(broadcast);
//        SingleOutputStreamOperator<SNStatusDto> process = connect.process(new KeyedBroadcastProcessFunction<Object, SNStatusDto, AlarmRuleDto, SNStatusDto>() {
//            @Override
//            public void processElement(SNStatusDto value, ReadOnlyContext ctx, Collector<SNStatusDto> out) throws Exception {
//                ReadOnlyBroadcastState<Object, Object> broadcastState = ctx.getBroadcastState(null);
//            }
//
//            @Override
//            public void processBroadcastElement(AlarmRuleDto value, Context ctx, Collector<SNStatusDto> out) throws Exception {
//                BroadcastState<Object, Object> broadcastState = ctx.getBroadcastState(null);
//
//            }
//
//            @Override
//            public void onTimer(long timestamp, OnTimerContext ctx, Collector<SNStatusDto> out) throws Exception {
//                super.onTimer(timestamp, ctx, out);
//            }
//        });
//
//
//        try {
//            env.execute();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    static class RuleFilter extends RichFilterFunction<SNStatusDto> implements CheckpointedFunction{
//        private MapStateDescriptor<String, JSONObject> stateDescriptor;
//        private FunctionInitializationContext context;
//
//        public RuleFilter(MapStateDescriptor<String, JSONObject> stateDescriptor) {
//            this.stateDescriptor = stateDescriptor;
//        }
//
//        @Override
//        public boolean filter(SNStatusDto snStatusDto) throws Exception {
//            List<Object> snTemplateDatapoint = getRuntimeContext().getBroadcastVariable("snTemplateDatapoint");
//            System.out.println(snTemplateDatapoint);
//            return true;
//
//        }
//
//        @Override
//        public void snapshotState(FunctionSnapshotContext context) throws Exception {
//
//        }
//
//        @Override
//        public void initializeState(FunctionInitializationContext context) throws Exception {
//            this.context = context;
//
//        }
//    }
//
//
//    /***
//     * 只是 动态更新规则
//     */
//    static class FillingSnAlaramBroadcastProcessFunction extends BroadcastProcessFunction<SNStatusDto, AlarmRuleDto, SNStatusDto> {
//        MapStateDescriptor<String, JSONObject> stateDescriptor;
//
//        public FillingSnAlaramBroadcastProcessFunction(MapStateDescriptor<String, JSONObject> stateDescriptor) {
//            this.stateDescriptor = stateDescriptor;
//        }
//
//        @Override
//        public void processElement(SNStatusDto value, ReadOnlyContext ctx, Collector<SNStatusDto> out) throws Exception {
//            out.collect(value);
//        }
//
//        /***
//         * 实时监控规则变化
//         * @param value
//         * @param ctx
//         * @param out
//         * @throws Exception
//         */
//        @Override
//        public void processBroadcastElement(AlarmRuleDto value, Context ctx, Collector<SNStatusDto> out) throws Exception {
//            BroadcastState<String, JSONObject> broadcastState = ctx.getBroadcastState(stateDescriptor);
//            String sn = value.getSn();
//            broadcastState.put(sn,value.getData());
//        }
//    }
//
//
//    /***
//     * 将数据转成 SNStatusDto
//     */
//    static class SNStatusMapper implements MapFunction<String, SNStatusDto> {
//        @Override
//        public SNStatusDto map(String s) throws Exception {
//            String[] split = s.split(",");
//            String sn = split[0];
//            String statusVal = split[1];
//            String time = split[2];
//            return new SNStatusDto(Long.parseLong(time),statusVal,sn);
//        }
//    }
//}
