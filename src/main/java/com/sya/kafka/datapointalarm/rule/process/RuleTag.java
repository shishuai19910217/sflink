package com.sya.kafka.datapointalarm.rule.process;

import com.sya.cache.RedisUtil;
import com.sya.cache.RuleCacheUtil;
import com.sya.constants.RuleCacheConstant;
import com.sya.dto.RuleBaseDto;
import com.sya.kafka.datapointalarm.rule.dto.DataPointDto;
import com.sya.utils.CommonUtil;
import com.sya.utils.IdUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/***
 * 为数据流添加规则标志
 */
public class RuleTag implements Serializable {



    public static DataStream<DataPointDto> exec(DataStream<DataPointDto> dataStream){
        /***
         * 报警规则打便签
         */
        KeyedStream<DataPointDto, String> keyedStream = dataStream.keyBy(new KeySelector<DataPointDto, String>() {
            @Override
            public String getKey(DataPointDto dataPointDto) throws Exception {
                return dataPointDto.getUniqueDataPointId().toString();
            }
        });
        // 规则打标
        SingleOutputStreamOperator<DataPointDto> process = keyedStream.process(new KeyedProcessFunctionTag());
        // 数据过滤
        SingleOutputStreamOperator<DataPointDto> filter = process.filter(new FilterFunction<DataPointDto>() {
            @Override
            public boolean filter(DataPointDto dataPointDto) throws Exception {
                if (CommonUtil.judgeEmpty(dataPointDto.getRuleIds())) {
                    return false;
                } else {
                    return true;
                }

            }
        });
        // 数据压平
        DataStream<DataPointDto> dataPointDtoSingleOutputStreamOperator = filter.flatMap(new FlatMapFunctionTag());
        // 除去控制的定时任务
        DataStream<DataPointDto> data = dataPointDtoSingleOutputStreamOperator.filter(action -> {
            String triggerType = action.getRuleBaseDto().getTriggerType();
            if ("3".equals(triggerType)) {
                return false;
            }
            return true;

        });
        return data;
    }

    /****
     * 规则打标 -》 DataPointDto.ruleIds
     * ","分隔
     */
    static class KeyedProcessFunctionTag extends KeyedProcessFunction<String, DataPointDto, DataPointDto> {

        private static final String valueStatePrefix = "TagKeyed-";
        @Override
        public void processElement(DataPointDto value, Context ctx, Collector<DataPointDto> out) throws Exception {
            Integer uniqueDataPointId = value.getUniqueDataPointId();
            Integer datapointId = value.getDatapointId();
            ValueStateDescriptor<String> valueStateDescriptor =
                    new ValueStateDescriptor(valueStatePrefix+RuleCacheConstant.RULE_UNIQUEDATAPOINT_CACHE_PREFIX+uniqueDataPointId, String.class);
            StateTtlConfig build = StateTtlConfig.newBuilder(Time.milliseconds(5000)).build();
            valueStateDescriptor.enableTimeToLive(build);
            ValueState<String> state = getRuntimeContext().getState(valueStateDescriptor);
            String ruleIds = state.value();
            if (CommonUtil.judgeEmpty(ruleIds)) {
                String uniqueDataPointRuleMapper = RedisUtil.getUniqueDataPointRuleMapper(uniqueDataPointId.toString());
                String dataPointRuleMapper = RedisUtil.getDataPointRuleMapper(datapointId.toString());
                StringBuffer rulemapper = new StringBuffer("");
                if (!CommonUtil.judgeEmpty(uniqueDataPointRuleMapper)) {
                    rulemapper.append(uniqueDataPointRuleMapper+",");
                }
                if (!CommonUtil.judgeEmpty(dataPointRuleMapper)) {
                    rulemapper.append(dataPointRuleMapper);
                }
                if (!CommonUtil.judgeEmpty(rulemapper.toString())) {
                    state.update(rulemapper.toString());
                    ruleIds = state.value();
                }
            }
            if (CommonUtil.judgeEmpty(ruleIds)) {
                return;
            }
            value.setRuleIds(ruleIds);
            out.collect(value);

        }


    }

    
    static class FlatMapFunctionTag implements FlatMapFunction<DataPointDto,DataPointDto>{
        @Override
        public void flatMap(DataPointDto dataPointDto, Collector<DataPointDto> collector) throws Exception {
            String ruleIds = dataPointDto.getRuleIds();
            String[] split = ruleIds.split(",");
            for (String s : split) {
                DataPointDto newData = new DataPointDto(dataPointDto.getSn(), dataPointDto.getDatapointId(), dataPointDto.getUniqueDataPointId()
                        , dataPointDto.getTemplateId(), dataPointDto.getVal());
                newData.setRuleId(s);

                RuleBaseDto ruleBaseData = RuleCacheUtil.getRuleBaseData(s);
                newData.setRuleBaseDto(ruleBaseData);
                newData.setRuleMachineId(ruleBaseData.getElementList().get(0).getMachineId());
                newData.setUuid(IdUtils.getId().toString());
                collector.collect(newData);
            }
        }
    }

}
