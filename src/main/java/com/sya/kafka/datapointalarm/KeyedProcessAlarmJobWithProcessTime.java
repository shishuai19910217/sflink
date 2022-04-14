package com.sya.kafka.datapointalarm;

import com.alibaba.fastjson.JSONObject;
import com.sya.cache.RedisUtil;
import com.sya.cache.RuleCacheUtil;
import com.sya.config.MybatisConfig;
import com.sya.dto.RuleBaseCacheDto;
import com.sya.dto.RuleMonitorElement;
import com.sya.dto.SnAlaram;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
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
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class KeyedProcessAlarmJobWithProcessTime {
    public static void main(String[] args) throws Exception {

        // 获取 flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("localhost", 9999);
        //env.setParallelism(1);
        //env.setStateBackend(new RocksDBStateBackend())
        // 设置状态后端
        OutputTag<DataPointDto> alramTag = new OutputTag<DataPointDto>("sn-alarm"){};
        /***
         * 数据转换 流分离  初始报警流
         */
        DataStream<DataPointDto> mapStream = stringDataStreamSource.map(new DataPointMapper());

        // 广播流 --规则
        // 为规则打标签
        MapStateDescriptor<String, String> stateDescriptor = new MapStateDescriptor<>("state", String.class, String.class);
        DataStreamSource<String> rules = env.socketTextStream("localhost", 2222);
        BroadcastStream<String> broadcast = rules.broadcast(stateDescriptor);

        KeyedStream<DataPointDto, String> keyedStream = mapStream.keyBy(new KeySelector<DataPointDto, String>() {
            @Override
            public String getKey(DataPointDto o) throws Exception {
                return o.getSn();
            }
        });
        BroadcastConnectedStream<DataPointDto, String> connect = keyedStream.connect(broadcast);
        // 动态修改规则
        SingleOutputStreamOperator<DataPointDto> process = connect.process(new KeyedBroadcastProcessFunction<DataPointDto, DataPointDto, String, DataPointDto>() {
            @Override
            public void processElement(DataPointDto value, ReadOnlyContext ctx, Collector<DataPointDto> out) throws Exception {
                out.collect(value);
            }


            /***
             * {
             *     id:123 ,#######变量id
             *     type: 0,   ######id类型0：代表变量id为unique_data_point_id  ；1：代表变量id为data_point_id；2：代表变量id为opera_data_point_id
             *    action_type:  0   ########操作类型0:新增；1：修改；2：删除
             *  ruleId:"1,2,3" #######规则id列表
             * }
             * @param value
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<DataPointDto> out) throws Exception {
                /***
                 * 接受 规则动态变更 更新redis
                 */
                JSONObject pointRule = JSONObject.parseObject(value);
                String type = pointRule.getString("type");
                Integer id = pointRule.getInteger("id");
                String actionType = pointRule.getString("actionType");
                if ("0".equals(type)) {
                   // unique_data_point_id
                    List<RuleMonitorElement> list = RuleCacheUtil.getRuleMonitorElementForDB(id);
                    if (null == list || list.size() <= 0) {
                        return;
                    }
                    String ruleIdStr = list.stream().map(action -> {
                        return action.getRuleId().toString();
                    }).collect(Collectors.joining(","));
                    String sn = list.get(0).getSn();
                    List<Integer> ruleIds = list.stream().map(action -> {
                        return action.getRuleId();
                    }).collect(Collectors.toList());
                    RedisUtil.setUniqueDataPointRuleMapper(sn,id.toString(),ruleIdStr);
                    List<RuleBaseCacheDto> ruleBaseCacheDtoList = RuleCacheUtil.getBaseRuleForDB(ruleIds);
                }


            }
        });
        /***
         * 报警规则打便签
         */
        KeyedStream<DataPointDto, String> dataPointDtoStringKeyedStream = process.keyBy(new KeySelector<DataPointDto, String>() {
            @Override
            public String getKey(DataPointDto dataPointDto) throws Exception {
                return dataPointDto.getSn();
            }
        });
        SingleOutputStreamOperator<DataPointDto> singleOutputStreamOperator = dataPointDtoStringKeyedStream.process(new TagKeyedProcessFunction());
        //根据规则 将流扁平化处理
        SingleOutputStreamOperator<DataPointDto> dataPointDtoSingleOutputStreamOperator = singleOutputStreamOperator.flatMap(new FlatMapFunction<DataPointDto, DataPointDto>() {
            @Override
            public void flatMap(DataPointDto dataPointDto, Collector<DataPointDto> collector) throws Exception {
                String alarmRuleIdFormonitorDev = dataPointDto.getAlarmRuleIdFormonitorDev();
                String[] split = null;
                if (null != alarmRuleIdFormonitorDev) {
                    split = alarmRuleIdFormonitorDev.toString().split(",");
                    for (String s : split) {
                        DataPointDto newData = new DataPointDto(dataPointDto.getSn(), dataPointDto.getDatapointId(), dataPointDto.getUniqueDataPointId()
                                , dataPointDto.getTemplateId(), dataPointDto.getVal());
                        newData.setRuleId(s);
                        newData.setDeviceId(dataPointDto.getDeviceId());
                        collector.collect(newData);
                    }
                }
                String alarmRuleIdFormonitorTemlate = dataPointDto.getAlarmRuleIdFormonitorTemlate();
                if (null != alarmRuleIdFormonitorTemlate) {
                    split = alarmRuleIdFormonitorTemlate.split(",");
                    for (String s : split) {
                        DataPointDto newData = new DataPointDto(dataPointDto.getSn(), dataPointDto.getDatapointId(), dataPointDto.getUniqueDataPointId()
                                , dataPointDto.getTemplateId(), dataPointDto.getVal());
                        newData.setRuleId(s);
                        newData.setDeviceId(dataPointDto.getDeviceId());
                        collector.collect(newData);
                    }
                }

            }
        });
        // 按 数据点以及规则id 分区
        KeyedStream<DataPointDto, String> dataPointDtoStringKeyedStream1 = dataPointDtoSingleOutputStreamOperator.keyBy(new KeySelector<DataPointDto, String>() {
            @Override
            public String getKey(DataPointDto dataPointDto) throws Exception {
                if (null == dataPointDto.getRuleId()) {
                    return dataPointDto.getUniqueDataPointId().toString();
                }
                return dataPointDto.getUniqueDataPointId().toString() + "-" + dataPointDto.getRuleId();
            }
        });
        // 加载报警规则  产生【报警流】
        SingleOutputStreamOperator<DataPointDto> process1 = dataPointDtoStringKeyedStream1.process(new SingleOutKeyedProcessFunction());

        // 【报警流-恢复流】   1 1 1 0 1 1 1
        DataStream<DataPointDto> sideOutput = process1.getSideOutput(alramTag);
        sideOutput.print(); // 其实是报警信息入库





        // 定义【时钟流】推动flink 向下处理   并流合并 TODO 暂时不需要
        // 处理推送流
//        KeyedStream<DataPointDto, String> dataPointDtoStringKeyedStream2 = sideOutput.keyBy(new KeySelector<DataPointDto, String>() {
//            @Override
//            public String getKey(DataPointDto dataPointDto) throws Exception {
//                return dataPointDto.getUniqueDataPointId().toString() + "-"+ dataPointDto.getRuleId();
//            }
//        });
//
//
//        SingleOutputStreamOperator<DatapointAlarmPushMessageDto> process2 = dataPointDtoStringKeyedStream2.process(new KeyedProcessFunction<String, DataPointDto, DatapointAlarmPushMessageDto>() {
//            @Override
//            public void onTimer(long timestamp, OnTimerContext ctx, Collector<DatapointAlarmPushMessageDto> out) throws Exception {
//                String currentKey = ctx.getCurrentKey();
//
//                String ruleId = currentKey.split("-")[1];
//                String uniqueDataPointId = currentKey.split("-")[0];
//                ValueStateDescriptor<Map<String, Object>> valueStateDescriptor = new ValueStateDescriptor("datapointalarmpush_"+currentKey, Map.class);
//                ValueState<Map<String, Object>> state = getRuntimeContext().getState(valueStateDescriptor);
//                /***
//                 * key 为time 值为 定时器处理时间
//                 * key 为 data 值为本次报警标志
//                 */
//                Map<String, Object> mapState = state.value();
//                DataPointDto data = (DataPointDto) mapState.get("data");
//                DatapointAlarmPushMessageDto dto = new DatapointAlarmPushMessageDto(uniqueDataPointId, data.getSn(), "", data.getMsg(), ruleId);
//                out.collect(dto);
//                // 计算下次报警时间
//                AlarmRuleDto alarmRuleData = RedisUtil.getAlarmRuleData(ruleId);
//                int pushInterval = alarmRuleData.getPushInterval() * 1000;
//                long time = pushInterval + ctx.timerService().currentProcessingTime();
//                mapState.put("time", time);
//                state.update(mapState);
//                ctx.timerService().registerProcessingTimeTimer(time);
//            }
//
//            @Override
//            public void processElement(DataPointDto value, Context ctx, Collector<DatapointAlarmPushMessageDto> out) throws Exception {
//                String currentKey = ctx.getCurrentKey();
//                String ruleId = currentKey.split("-")[1];
//                String uniqueDataPointId = currentKey.split("-")[0];
//                AlarmRuleDto alarmRuleData = RedisUtil.getAlarmRuleData(ruleId);
//                // 需要推送
//                if ("1".equals(alarmRuleData.getPushStatus())) {
//
//                    int delayTime = alarmRuleData.getDelayTime();
//                    // 实时报警 就不需要关注恢复了
//                    if (delayTime <= 0) {
//                        if (!value.getRecover()) {
//                            // 仅推送一次
//                            if ("0".equals(alarmRuleData.getPushFrequency())) {
//                                if (!RedisUtil.existsDatapointAlaramPush(uniqueDataPointId,ruleId)) {
//                                    DatapointAlarmPushMessageDto dto = new DatapointAlarmPushMessageDto(uniqueDataPointId, value.getSn(), "", value.getMsg(), ruleId);
//                                    out.collect(dto);
//                                    RedisUtil.setDatapointAlaramPushStatus(uniqueDataPointId,ruleId);
//                                }
//                            }else {
//                                DatapointAlarmPushMessageDto dto = new DatapointAlarmPushMessageDto(uniqueDataPointId, value.getSn(), "", value.getMsg(), ruleId);
//                                out.collect(dto);
//                                RedisUtil.delDatapointAlaramPushStatus(uniqueDataPointId,ruleId);
//
//                            }
//                        }
//
//                    }else {
//
//                        if (!value.getRecover()) {
//
//                            // 又需要定时器了。。。。。我疯了
//                            // 推送间隔
//                            int pushInterval = alarmRuleData.getPushInterval() * 1000;
//                            ValueStateDescriptor<Map<String, Object>> valueStateDescriptor = new ValueStateDescriptor("datapointalarmpush_"+currentKey, Map.class);
//                            ValueState<Map<String, Object>> state = getRuntimeContext().getState(valueStateDescriptor);
//                            /***
//                             * key 为time 值为 定时器处理时间
//                             * key 为 data 值为本次报警标志
//                             */
//                            Map<String, Object> mapState = state.value();
//                            if (null == mapState) {
//                                // 说明不存在定时器 开启定时器
//                                long currentProcessingTime = ctx.timerService().currentProcessingTime();
//                                long t = currentProcessingTime + pushInterval;
//                                ctx.timerService().registerProcessingTimeTimer(t);
//                                mapState = new HashMap<>(2);
//                                mapState.put("time", t);
//                                mapState.put("data", value);
//                                state.update(mapState);
//
//                            }else {
//                                // 更新 数据 推送时间间隔中最新的数据 （可以redis ）
//                                mapState.put("data", value);
//                                state.update(mapState);
//                            }
//                         }else {
//                            // 恢复了  就不推送了  删除定时器；缓存记录就行了
//                            ValueStateDescriptor<Map<String, Object>> valueStateDescriptor = new ValueStateDescriptor("datapointalarmpush_"+currentKey, Map.class);
//                            ValueState<Map<String, Object>> state = getRuntimeContext().getState(valueStateDescriptor);
//                            /***
//                             * key 为time 值为 定时器处理时间
//                             * key 为 data 值为本次报警标志
//                             */
//                            Map<String, Object> mapState = state.value();
//                            if (null != mapState) {
//                                long time = Long.parseLong(mapState.get("time").toString());
//                                ctx.timerService().deleteProcessingTimeTimer(time);
//                                state.clear();
//                            }
//
//                        }
//
//
//                    }
//
//                }
//
//
//            }
//        });
//        // 推送信息入库
//        process2.print();


        // 控制处理





        env.execute();
    }

    static class TagKeyedProcessFunction extends KeyedProcessFunction<String, DataPointDto, DataPointDto> {
        @Override
        public void processElement(DataPointDto value, Context ctx, Collector<DataPointDto> out) throws Exception {
            String sn = value.getSn();
            Integer uniqueDataPointId = value.getUniqueDataPointId();
            ValueStateDescriptor<Map<String,String>> mapValueStateDescriptor = new ValueStateDescriptor(sn + "forDevice", Map.class);
            ValueState<Map<String,String>> state = getRuntimeContext().getState(mapValueStateDescriptor);
            //123:"1,2,3", // 123 代表unique_data_point_id ；1,2,3 代表变量报警规则的主键id
            Map<String, String> alarmRuleForSn = state.value();
            if (null == alarmRuleForSn) {
                // 重新在redis获取
                alarmRuleForSn = RedisUtil.getDeviceModelAlaramRuleIds(sn);
                state.update(alarmRuleForSn);
            }
            if (alarmRuleForSn.containsKey(uniqueDataPointId.toString())) {
                value.setAlarmRuleIdFormonitorDev(alarmRuleForSn.get(uniqueDataPointId.toString()));
            }
            // 设备打标
            value.setDeviceId(1);
            out.collect(value);
        }
    }




    /***
     * 报警频率 网关离线超过 N分钟
     *
     */
    static class MyAlarmKeyedProcessFunction extends KeyedProcessFunction<String, SnAlaram, SnAlaram> {
        public MyAlarmKeyedProcessFunction() {
            super();
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<SnAlaram> out) throws Exception {
            // 其实就是数据入库
            String currentKey = ctx.getCurrentKey();
            System.out.println(currentKey+"---报警了---"+ctx.timerService().currentProcessingTime());
            long timeInterval = 0L;
            if (currentKey.equals("a")) {
                timeInterval = 1000L;
            }else {
                timeInterval = 5000L;
            }
            ValueState<Long> sn_timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>(currentKey+"_alramTimerTs", Long.class));
            // 再设置 下次报警时间
            Long value = sn_timerTs.value();
            ctx.timerService().registerProcessingTimeTimer(value+timeInterval);
            sn_timerTs.update(value+timeInterval);

        }

        @Override
        public void processElement(SnAlaram value, Context ctx, Collector<SnAlaram> out) throws Exception {
            String sn = value.getSn();
            String status = value.getStatus();
            long timeInterval = 0L;

            if (sn.equals("a")) {
                timeInterval = 1000L;
            }else {
                timeInterval = 5000L;
            }
            /***
             * 起一个
             */
            // 定时任务
            ValueState<Long> sn_timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>(sn+"_alramTimerTs", Long.class));
            Long sn_timerTsVal = sn_timerTs.value();
            // 报警中
            if ("0".equals(status)) {
                if (null == sn_timerTsVal) {
                    // 计算出 定时任务的时间戳
                    long ts = ctx.timerService().currentProcessingTime();
                    ts = ts + timeInterval; // 五秒
                    ctx.timerService().registerProcessingTimeTimer(ts);
                    System.out.println("----所属的时间窗是---"+ts);
                    sn_timerTs.update(ts);
                }else {
                    System.out.println("----所属的时间窗是---"+sn_timerTsVal);
                }
            }else {
                // 删除 相关定时任务
                // 计算出 定时任务的时间戳
                if (null != sn_timerTsVal) {
                    ctx.timerService().deleteProcessingTimeTimer(sn_timerTsVal);
                    sn_timerTs.clear();
                }

            }
        }
    }


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




    /***
     * 将数据转成 DataPointDto
     */
    static class DataPointMapper implements MapFunction<String, DataPointDto> {
        @Override
        public DataPointDto map(String s) throws Exception {
            String[] split = s.split(",");
            String sn = split[0];
            Integer datapointId = Integer.parseInt(split[1]);
            Integer uniqueDataPointId = Integer.parseInt(split[2]);
            Integer templateId = Integer.parseInt(split[3]);
            String val = split[4];

            return new DataPointDto(sn,datapointId,uniqueDataPointId,templateId,val);
        }
    }
}
