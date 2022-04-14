package com.sya.kafka.datapointalarm.rule.process;

import com.alibaba.fastjson.JSONObject;
import com.sya.cache.RuleCacheUtil;
import com.sya.config.PropertiesUnit;
import com.sya.dto.RuleAction;
import com.sya.dto.RuleBaseDto;
import com.sya.dto.RuleMonitorElement;
import com.sya.kafka.datapointalarm.rule.dto.*;
import com.sya.utils.CommonUtil;
import com.sya.utils.IdUtils;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;

public class ControlProcess implements Serializable {

    public static DataStream<DatapointAlarmPushMessageDto> exec(DataStream<DataPointDto> alramStream) {
        // 由报警触发的控制流
        SingleOutputStreamOperator<DataPointDto> filter = alramStream.filter(action -> {
            return "2".equals(action.getRuleBaseDto().getTriggerType());
        });
        // 控制规则亚平
        SingleOutputStreamOperator<DataPointDto> dataPointDtoSingleOutputStreamOperator = filter.flatMap(new FlatMapFunction<DataPointDto, DataPointDto>() {
            @Override
            public void flatMap(DataPointDto dataPointDto, Collector<DataPointDto> collector) throws Exception {
                // 根据报警规则查找关联的控制规则
                String relyRuleId = dataPointDto.getRuleBaseDto().getRelyRuleId();
                List<Integer> controlIds = RuleCacheUtil.getRuleBaseDataByRelyRuleId(relyRuleId);
                if (CommonUtil.judgeEmpty(controlIds)) {
                    return;
                }
                DataPointDto newData = null;
                for (Integer controlId : controlIds) {
                    newData = new DataPointDto();
                    BeanUtils.copyProperties(dataPointDto, newData);
                    newData.setRuleId(controlId.toString());
                    newData.setRuleBaseDto(RuleCacheUtil.getRuleBaseData(controlId.toString()));
                    collector.collect(newData);
                }

            }
        });




        return null;
    }
    static class ControlProcessFunction extends ProcessFunction<DataPointDto, ControlDto> {
        private static final   Properties properties = PropertiesUnit.getProperties("aplication.properties");
        @Override
        public void processElement(DataPointDto value, Context ctx, Collector<ControlDto> out) throws Exception {
            String selector = properties.getProperty("rule.action.obj");

            RuleBaseDto ruleBaseDto = value.getRuleBaseDto();
            RuleAction ruleAction = ruleBaseDto.getRuleAction();
            String action = ruleAction.getAction();
            ActionDto actionDto = JSONObject.parseObject(action, ActionDto.class);
            String trigger = ruleBaseDto.getTrigger();
            List<RuleMonitorElement> elementList = ruleBaseDto.getElementList();
            List<ControlActionDto> controlActionDtos = actionDto.getControlActionDtos();
            if (CommonUtil.judgeEmpty(controlActionDtos)) {
                return;
            }
            /***
             * 匹配触发当前报警的对象
             */
            RuleMonitorElement ruleMonitorElement = null;
            for (RuleMonitorElement element : elementList) {
                if ("1".equals(element.getElementType())) {
                    // 网关
                    if (value.getUniqueDataPointId().intValue() == element.getUniqueDataPointId().intValue()){
                        ruleMonitorElement = element;
                        break;
                    }

                }else if ("1".equals(element.getElementType())) {
                    // 设备
                    if (value.getUniqueDataPointId().intValue() == element.getUniqueDataPointId().intValue()){
                        ruleMonitorElement = element;
                        break;
                    }
                }else if ("2".equals(element.getElementType())) {
                    // 模板
                    if (value.getDatapointId().intValue() == element.getDataPointId().intValue()){
                        ruleMonitorElement = element;
                        break;
                    }
                }
            }
            if (CommonUtil.judgeEmpty(ruleMonitorElement)) {
                return;
            }
            String elementType = ruleMonitorElement.getElementType();
            if ("1".equals(elementType)) {
                // 网关
                for (ControlActionDto controlActionDto : controlActionDtos) {
                    ControlDto dto = new ControlDto(IdUtils.getId().toString(),
                            value.getUuid(),controlActionDto.getControlData(),controlActionDto.getControlType(),trigger);
                    dto.setControlSn(value.getSn());
                    dto.setUniqueDataPointId(controlActionDto.getControlUniqueDataPointId());
                    out.collect(dto);
                }
            }else if ("1".equals(elementType)) {
                // 设备
                for (ControlActionDto controlActionDto : controlActionDtos) {
                    ControlDto dto = new ControlDto(IdUtils.getId().toString(),
                            value.getUuid(),controlActionDto.getControlData(),
                            controlActionDto.getControlType(),trigger);
                    dto.setControlSn(value.getSn());
                    dto.setUniqueDataPointId(controlActionDto.getControlUniqueDataPointId());
                    // 需要在 报警规则中获取
                    dto.setControlMachineId(ruleMonitorElement.getMachineId());
                    out.collect(dto);
                }
            }else if ("2".equals(elementType)) {
                // 模板
                for (ControlActionDto controlActionDto : controlActionDtos) {
                    ControlDto dto = new ControlDto(IdUtils.getId().toString(),
                            value.getUuid(),controlActionDto.getControlData(),
                            controlActionDto.getControlType(),trigger);
                    dto.setControlSn(value.getSn());
                    // 定位 数采设备
                    dto.setControlMachineId(null);
                    // 根据 sn +datapointid 定位 UniqueDataPointId
                    Integer controlDataPointId = controlActionDto.getControlDataPointId();
                    dto.setUniqueDataPointId();
                    out.collect(dto);
                }

            }


        }
    }
}
