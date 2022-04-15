package com.sya.kafka.datapointalarm.rule;

import com.alibaba.fastjson.JSONObject;
import com.sya.cache.RedisUtil;
import com.sya.cache.RuleCacheUtil;
import com.sya.config.PropertiesUnit;
import com.sya.dto.RuleAction;
import com.sya.dto.RuleBaseDto;
import com.sya.dto.SnAlaram;
import com.sya.kafka.datapointalarm.AlarmRuleDto;
import com.sya.kafka.datapointalarm.rule.dto.*;
import com.sya.kafka.datapointalarm.rule.process.*;
import com.sya.kafka.datapointalarm.rule.conversion.KafkaDataFlatMapper;
import com.sya.kafka.datapointalarm.rule.source.SourceBuilder;
import com.sya.utils.CommonUtil;
import com.sya.utils.IdUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class KeyedProcessAlarmJobWithProcessTime {
    /***
     * 规则动态的标签
     */
    private static  final  MapStateDescriptor<String, String> stateDescriptor
            = new MapStateDescriptor<>("state", String.class, String.class);

    public static void main(String[] args) throws Exception {
        // 获取 flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);
        //env.setStateBackend(new RocksDBStateBackend())
        // 获取kafka中数据点数据（不包含计算型变量）
        DataStream<DataPointDto> kafkaPointData = getKafkaPointData(env);
        // 规则动态修改
        DataStream<DataPointDto> dynamicModifyRule = DynamicModifyRule.exec(kafkaPointData, env);
        // 规则打标，压平 只产出含有规则id的数据
        DataStream<DataPointDto> ruleTag = RuleTag.exec(dynamicModifyRule);
        // 按 数据点以及规则id 分区
        KeyedStream<DataPointDto, String> keyedStream = ruleTag.keyBy(new KeySelector<DataPointDto, String>() {
            @Override
            public String getKey(DataPointDto dataPointDto) throws Exception {
                String key = dataPointDto.getUniqueDataPointId().toString() + "-" + dataPointDto.getRuleId();
                return key;
            }
        });
        // 加载报警规则  产生【动作流】
        SingleOutputStreamOperator<DataPointDto> triggerProcess = TriggerProcess.exec(keyedStream, env);
        // 【报警流-恢复流】   1 1 1 0 1 1 1
        DataStream<DataPointDto> alramStream = triggerProcess.getSideOutput(TriggerProcess.SingleOutKeyedProcessFunction.alramTag);
        // 依赖报警的控制处理
        DataStream<ControlDto> controlDtoDataStreamForAlarm = ControlProcess.execForAlarm(alramStream);
        // 【控制流】
        DataStream<DataPointDto> controlStream = triggerProcess.getSideOutput(TriggerProcess.SingleOutKeyedProcessFunction.controlTag);
        DataStream<ControlDto> controlDtoDataStream = ControlProcess.exec(controlStream);
        alramStream.print(); // 其实是报警信息入库
        // 控制入库
        controlDtoDataStreamForAlarm.print();
        // 定义【时钟流】推动flink 向下处理   并流合并 TODO 暂时不需要
        // 处理推送流
        KeyedStream<DataPointDto, String> pushKeyByStream = alramStream.keyBy(new KeySelector<DataPointDto, String>() {
            @Override
            public String getKey(DataPointDto dataPointDto) throws Exception {
                return dataPointDto.getUniqueDataPointId().toString() + "-"+ dataPointDto.getRuleId();
            }
        });
        DataStream<DatapointAlarmPushMessageDto> pushProcess = PushProcess.exec(pushKeyByStream, env);
        // 推送信息入库
        pushProcess.print();
        env.execute();
    }

    /****
     * 获取 kafka 数据点上报原始流
     * @param env
     * @return
     */
    public static   DataStream<DataPointDto> getKafkaPointData(StreamExecutionEnvironment env){
        FlinkKafkaConsumer kafkaSource = SourceBuilder.getKafkaPointDataSource();
        DataStreamSource<JSONObject> dataStreamSource = env.addSource(kafkaSource);
        DataStream<DataPointDto> dataPointDto = dataStreamSource.flatMap(new KafkaDataFlatMapper());
        return dataPointDto;
    }







    /***
     * 报警频率 网关离线超过 N分钟
     *
     */


    static class SingleOutKeyedProcessFunction  extends KeyedProcessFunction<String, DataPointDto, DataPointDto>  implements CheckpointedFunction{
        // 报警流标志
        private   OutputTag<DataPointDto> alramTag = new OutputTag<DataPointDto>("sn-alarm"){};
        //

        @Override
        public void processElement(DataPointDto value, Context ctx, Collector<DataPointDto> out) throws Exception {
            // dataPointDto.getUniqueDataPointId().toString() + "-" + dataPointDto.getRuleId()
            String currentKey = ctx.getCurrentKey();
            String ruleId = currentKey.split("-")[1];
            String uniqueDataPointId =  currentKey.split("-")[0];
            if (null != ruleId) {
                AlarmRuleDto rule = RedisUtil.getAlarmRuleData(ruleId);
                {
                    // 获取 延期时间
                    Integer delayTime = rule.getDelayTime()*1000;

                    if (alarmStatus(value,rule)) {
                        if (!delayTime.equals("0")) {
                            /***
                             * 说明存在延期推送;也就是需要定时器
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
                                value.setMsg("触发了报警,报警id为"+ruleId);
                                mapState.put("data",value);
                                state.update(mapState);
                            }else {
                                //报警信息临时存储
                                value.setMsg("触发了报警,报警id为"+ruleId);
                                mapState.put("data",value);
                                state.update(mapState);
                            }

                        }else {
                            // 直接发送报警记录
                            ctx.output(alramTag,value);
                            /***
                             *  获取当前数据点当前规则是否报过警 如果报过才会有恢复
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
                                if (RedisUtil.existsLastAlarmStatus(uniqueDataPointId,ruleId)) {
                                    value.setMsg("解除了报警,报警id为"+ruleId);
                                    value.setStorage(true);
                                    value.setRecover(true);
                                    ctx.output(alramTag,value);
                                    RedisUtil.delLastAlarmStatus(uniqueDataPointId,ruleId);
                                }

                            }else {
                                // 恢复了 直接发送 从已经报警之后恢复才有意义
                                if (RedisUtil.existsLastAlarmStatus(uniqueDataPointId,ruleId)) {
                                    value.setMsg("解除了报警,报警id为"+ruleId);
                                    value.setStorage(true);
                                    value.setRecover(true);
                                    ctx.output(alramTag,value);
                                    RedisUtil.delLastAlarmStatus(uniqueDataPointId,ruleId);
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
            RedisUtil.setLastAlarmStatus(uniqueDataPointId,ruleId, JSONObject.toJSONString(data));




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

        public boolean alarmStatus(DataPointDto value,AlarmRuleDto rule){
            if (rule == null) {
                return false;
            }
            String triggerCondition = rule.getTriggerCondition();
            // 数字等于A
            if ("2".equals(triggerCondition)) {
                String triggerConditionAVal = rule.getTriggerConditionAVal();
                if (value.getVal().equals(triggerConditionAVal)) {
                    return true;
                }
            }
            return false;
        }

    }




}
