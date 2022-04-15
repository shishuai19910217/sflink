package com.sya.kafka.datapointalarm.rule.process;

import com.alibaba.fastjson.JSONObject;
import com.sya.cache.CacheUtil;
import com.sya.cache.PointDataCacheUtil;
import com.sya.cache.PointDataCacheUtil;
import com.sya.dto.RuleAction;
import com.sya.dto.RuleBaseDto;
import com.sya.kafka.datapointalarm.AlarmRuleDto;
import com.sya.kafka.datapointalarm.rule.dto.DataPointDto;
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
import org.apache.ibatis.annotations.Case;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class TriggerProcess implements Serializable {

    public static SingleOutputStreamOperator<DataPointDto> exec(KeyedStream<DataPointDto, String> keyedStream, StreamExecutionEnvironment env) {

        SingleOutputStreamOperator<DataPointDto> process = keyedStream.process(new SingleOutKeyedProcessFunction());


        return process;
    }

    public static class SingleOutKeyedProcessFunction  extends KeyedProcessFunction<String, DataPointDto, DataPointDto> implements CheckpointedFunction {
        // 报警流标志
        public static final OutputTag<DataPointDto> alramTag = new OutputTag<DataPointDto>("alramTag"){};

        // 控制（不包含由报警触发的控制）
        public static final OutputTag<DataPointDto> controlTag = new OutputTag<DataPointDto>("controlTag"){};


        @Override
        public void processElement(DataPointDto value, Context ctx, Collector<DataPointDto> out) throws Exception {
            // dataPointDto.getUniqueDataPointId().toString() + "-" + dataPointDto.getRuleId()
            String currentKey = ctx.getCurrentKey();
            String[] split = currentKey.split("-");
            String ruleId = split[1];
            String uniqueDataPointId =  split[0];

            RuleBaseDto rule = value.getRuleBaseDto();
            RuleAction ruleAction = rule.getRuleAction();
            String actionType = ruleAction.getActionType();
            // 控制流
            if ("2".equals(actionType)) {
                // 不是由报警产生的
                if (!"2".equals(rule.getTriggerType())){
                    ctx.output(controlTag,value);
                }
                return;
            }
            // 报警流
            if (null != ruleId) {
                RuleTriggerDto trigger = JSONObject.parseObject(rule.getTrigger(), RuleTriggerDto.class);
                {
                    // 获取 延期时间
                    Integer delayTime = trigger.getDelayTime()*1000;
                    if (TriggerStatus.triggerStatus(value,trigger)) {
                        if (!delayTime.equals("0")) {
                            /***
                             * 说明存在延期;也就是需要定时器
                             */
                            /***
                             *  获取 当前报警规则下的定时器配置
                             *
                             */
                            ValueStateDescriptor<Map<String,Object>> valueStateDescriptor = new ValueStateDescriptor(currentKey,Map.class);
                            ValueState<Map<String, Object>> state =getRuntimeContext().getState(valueStateDescriptor);
                            /***
                             * key 为time 值为 定时器处理时间
                             * key 为 data 值为本次报警标志
                             */
                            Map<String, Object> mapState = state.value();

                            if (null == mapState) {
                                /**
                                 * 说明 未开启定时 要开启
                                 * 注册定时器
                                 * 更新本次报警状态
                                 */
                                long currentProcessingTime = ctx.timerService().currentProcessingTime();
                                long time = currentProcessingTime + delayTime;
                                ctx.timerService().registerProcessingTimeTimer(time);
                                mapState = new HashMap<>(2);
                                // 当前定时器时间
                                mapState.put("time",time);
                                //报警信息临时存储
                                value.setMsg(trigger.getAlarmMsg());
                                mapState.put("data",value);
                                state.update(mapState);
                            }else {
                                //报警信息临时存储
                                value.setMsg(trigger.getAlarmMsg());
                                mapState.put("data",value);
                                state.update(mapState);
                            }

                        }else {
                            // 没有 延时配置
                            // 直接发送报警记录
                            value.setMsg(trigger.getAlarmMsg());
                            ctx.output(alramTag,value);
                            /***
                             * 缓存当前报警的数据（没报过警就谈不到恢复）
                             */
                            ValueStateDescriptor<Map<String,Object>> valueStateDescriptor = new ValueStateDescriptor(currentKey,Map.class);
                            ValueState<Map<String, Object>> state =getRuntimeContext().getState(valueStateDescriptor);
                            /***
                             * key 为 data 值为本次报警标志
                             */
                            Map<String, Object> mapState = state.value();
                            if (null != mapState) {
                                // 生成一个state
                                mapState = new HashMap<>(2);
                                mapState.put("data",value);
                            }
                            //更新本次报警
                            state.update(mapState);
                        }
                    }else {
                        // 恢复
                        if (!delayTime.equals("0")) {
                            /***
                             * 删除 相关定时任务
                             */
                            /***
                             *  获取 当前报警规则下的存储状态
                             *
                             */
                            ValueStateDescriptor<Map<String,Object>> valueStateDescriptor = new ValueStateDescriptor(currentKey,Map.class);
                            ValueState<Map<String, Object>> state =getRuntimeContext().getState(valueStateDescriptor);
                            Map<String, Object> mapState = state.value();
                            if (null != mapState) {
                                // 在定时器范围内恢复 不需要发恢复记录
                                value.setMsg("解除了报警,报警id为"+ruleId);
                                Object time = mapState.get("time");
                                long l = Long.parseLong(time.toString());
                                ctx.timerService().deleteProcessingTimeTimer(l);
                                state.clear();
                                // 恢复了 直接发送 从已经报警之后恢复才有意义
                                if (PointDataCacheUtil.existsLastAlarmStatus(ruleId,uniqueDataPointId)) {
                                    value.setMsg("解除了报警,报警id为"+ruleId);
                                    value.setStorage(true);
                                    value.setRecover(true);
                                    ctx.output(alramTag,value);
                                    PointDataCacheUtil.delLastAlarmStatus(ruleId,uniqueDataPointId);
                                }

                            }else {
                                // 恢复了 直接发送 从已经报警之后恢复才有意义
                                if (PointDataCacheUtil.existsLastAlarmStatus(ruleId,uniqueDataPointId)) {
                                    value.setMsg("解除了报警,报警id为"+ruleId);
                                    value.setStorage(true);
                                    value.setRecover(true);
                                    ctx.output(alramTag,value);
                                    PointDataCacheUtil.delLastAlarmStatus(ruleId,uniqueDataPointId);
                                }
                            }

                        }else {
                            /***
                             *  获取当前数据点当前规则是否报过警 如果报过才会有恢复 应该放到 redis 用于状态汇总
                             *  没报过警就谈不到恢复
                             *
                             */
                            ValueStateDescriptor<Map<String,Object>> valueStateDescriptor = new ValueStateDescriptor(currentKey,Map.class);
                            ValueState<Map<String, Object>> state =getRuntimeContext().getState(valueStateDescriptor);
                            /***
                             * key 为 data 值为本次报警标志
                             */
                            Map<String, Object> mapState = state.value();
                            if (null != mapState) {
                                // 报过
                                value.setMsg("解除了报警,报警id为"+ruleId);
                                value.setStorage(true);
                                value.setRecover(true);
                                ctx.output(alramTag,value);
                            }
                        }



                    }
                }
            }



        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<DataPointDto> out) throws Exception {
            // 其实就是数据入库
            String currentKey = ctx.getCurrentKey();
            String ruleId = currentKey.split("-")[1];
            String uniqueDataPointId =  currentKey.split("-")[0];
            /***
             *  获取 当前报警规则下的存储状态
             *
             */
            ValueStateDescriptor<Map<String,Object>> valueStateDescriptor = new ValueStateDescriptor(currentKey,Map.class);
            ValueState<Map<String, Object>> state =getRuntimeContext().getState(valueStateDescriptor);
            Map<String, Object> value = state.value();
            long time = Long.parseLong(value.get("time").toString());
            ctx.timerService().deleteProcessingTimeTimer(time);
            // 报警数据
            DataPointDto data = (DataPointDto) value.get("data");
            ctx.output(alramTag, data);
            state.clear();
            // 把本次报警状态 缓存起来
            PointDataCacheUtil.setLastAlarmStatus(ruleId,uniqueDataPointId, JSONObject.toJSONString(data));



        }
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            //
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
        }



    }


}
