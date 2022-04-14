package com.sya.kafka.datapointalarm.rule.process;

import com.alibaba.fastjson.JSONObject;
import com.sya.cache.PointDataCacheUtil;
import com.sya.cache.PointDataCacheUtil;
import com.sya.dto.RuleAction;
import com.sya.dto.RuleBaseDto;
import com.sya.kafka.datapointalarm.AlarmRuleDto;
import com.sya.kafka.datapointalarm.rule.dto.ActionDto;
import com.sya.kafka.datapointalarm.rule.dto.DataPointDto;
import com.sya.kafka.datapointalarm.rule.dto.DatapointAlarmPushMessageDto;
import com.sya.kafka.datapointalarm.rule.dto.RuleTriggerDto;
import com.sya.utils.CommonUtil;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class PushProcess implements Serializable {
    public static DataStream<DatapointAlarmPushMessageDto> exec(KeyedStream<DataPointDto, String> keyedStream, StreamExecutionEnvironment env) {
        DataStream<DatapointAlarmPushMessageDto> process = keyedStream.process(new SingleOutKeyedProcessFunction());
        return process;
    }


    public static class SingleOutKeyedProcessFunction  extends KeyedProcessFunction<String, DataPointDto, DatapointAlarmPushMessageDto> implements CheckpointedFunction {

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<DatapointAlarmPushMessageDto> out) throws Exception {
            String currentKey = ctx.getCurrentKey();

            String ruleId = currentKey.split("-")[1];
            String uniqueDataPointId = currentKey.split("-")[0];
            ValueStateDescriptor<Map<String, Object>> valueStateDescriptor = new ValueStateDescriptor(currentKey, Map.class);
            ValueState<Map<String, Object>> state = getRuntimeContext().getState(valueStateDescriptor);
            /***
             * key 为time 值为 定时器处理时间
             * key 为 data 值为本次报警标志
             */
            Map<String, Object> mapState = state.value();
            DataPointDto data = (DataPointDto) mapState.get("data");
            DatapointAlarmPushMessageDto dto = new DatapointAlarmPushMessageDto(uniqueDataPointId, data.getSn(), "", data.getRuleBaseDto());
            RuleBaseDto rule = data.getRuleBaseDto();
            RuleAction ruleAction = rule.getRuleAction();
            ActionDto actionDto = JSONObject.parseObject(ruleAction.getAction(), ActionDto.class);
            int delayTime = 0;
            if (!CommonUtil.judgeEmpty(actionDto.getPushInterval())) {
                delayTime = actionDto.getPushInterval()*1000;
            }
            out.collect(dto);
            // 计算下次报警时间
            long time = delayTime + ctx.timerService().currentProcessingTime();
            mapState.put("time", time);
            state.update(mapState);
            ctx.timerService().registerProcessingTimeTimer(time);
        }

        @Override
        public void processElement(DataPointDto value, Context ctx, Collector<DatapointAlarmPushMessageDto> out) throws Exception {
            String currentKey = ctx.getCurrentKey();
            String ruleId = currentKey.split("-")[1];
            String uniqueDataPointId = currentKey.split("-")[0];
            RuleBaseDto rule = value.getRuleBaseDto();
            RuleAction ruleAction = rule.getRuleAction();
            String actionType = ruleAction.getActionType();
            // 需要推送的
            if ("3".equals(actionType)) {
                ActionDto actionDto = JSONObject.parseObject(ruleAction.getAction(), ActionDto.class);
                int delayTime = 0;
                if (!CommonUtil.judgeEmpty(actionDto.getPushInterval())) {
                    delayTime = actionDto.getPushInterval()*1000;
                }
                // 实时推送 (仅推送一次) 就不需要关注恢复了
                if (delayTime <= 0) {
                    if (!value.getRecover()) {
                        // 仅推送一次
                        Integer pushFrequency = actionDto.getPushFrequency();
                        if (pushFrequency.intValue()==0) {
                            if (!PointDataCacheUtil.existsDatapointAlaramPush(ruleId,uniqueDataPointId)) {

                                DatapointAlarmPushMessageDto dto = new DatapointAlarmPushMessageDto(uniqueDataPointId, value.getSn(), "", rule);
                                out.collect(dto);
                                // 这个缓存在 规则变更中更新
                                PointDataCacheUtil.setDatapointAlaramPushStatus(ruleId,uniqueDataPointId);
                            }
                        }
                    }

                }else {

                    if (!value.getRecover()) {

                        // 又需要定时器了。。。。。我疯了
                        // 推送间隔
                        ValueStateDescriptor<Map<String, Object>> valueStateDescriptor = new ValueStateDescriptor(currentKey, Map.class);
                        ValueState<Map<String, Object>> state = getRuntimeContext().getState(valueStateDescriptor);
                        /***
                         * key 为time 值为 定时器处理时间
                         * key 为 data 值为本次报警标志
                         */
                        Map<String, Object> mapState = state.value();
                        if (null == mapState) {
                            // 说明不存在定时器 开启定时器
                            long currentProcessingTime = ctx.timerService().currentProcessingTime();
                            long t = currentProcessingTime + delayTime;
                            ctx.timerService().registerProcessingTimeTimer(t);
                            mapState = new HashMap<>(2);
                            mapState.put("time", t);
                            mapState.put("data", value);
                            state.update(mapState);

                        }else {
                            // 更新 数据 推送时间间隔中最新的数据 （可以redis ）
                            mapState.put("data", value);
                            state.update(mapState);
                        }
                    }else {
                        // 恢复了  就不推送了  删除定时器；缓存记录就行了
                        ValueStateDescriptor<Map<String, Object>> valueStateDescriptor = new ValueStateDescriptor(currentKey, Map.class);
                        ValueState<Map<String, Object>> state = getRuntimeContext().getState(valueStateDescriptor);
                        /***
                         * key 为time 值为 定时器处理时间
                         * key 为 data 值为本次报警标志
                         */
                        Map<String, Object> mapState = state.value();
                        if (null != mapState) {
                            long time = Long.parseLong(mapState.get("time").toString());
                            ctx.timerService().deleteProcessingTimeTimer(time);
                            state.clear();
                        }

                    }


                }

            }


        }
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
        }



    }




}
