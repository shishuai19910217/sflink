package com.sya.kafka.datapointalarm.rule.process;

import com.sya.kafka.datapointalarm.rule.dto.DataPointDto;
import com.sya.kafka.datapointalarm.rule.dto.RuleTriggerDto;
import com.sya.utils.CommonUtil;

public class TriggerStatus {
    public static boolean triggerStatus(DataPointDto value, RuleTriggerDto triggerDto){
        if (triggerDto == null) {
            return false;
        }
        String triggerCondition = triggerDto.getTriggerCondition();
        String val = value.getVal().trim();
        if ("0".equals(triggerCondition)) {
            // 开启
            if ("1".equals(val)) {
                return true;
            }
        }
        if ("1".equals(triggerCondition)) {
            // 开关OFF
            if ("0".equals(val)) {
                return true;
            }
        }

        if ("2".equals(triggerCondition)) {
            // 数值等于A
            Integer triggerConditionValType = triggerDto.getTriggerConditionValType();
            if (triggerConditionValType.intValue() == 0) {
                // 数值类型
                Double num = CommonUtil.str2Double(val);
                if (num.doubleValue() == triggerConditionValType) {
                    return true;
                }
            }else if (triggerConditionValType.intValue() == 1) {
                // 字符串
                if (val.equals(triggerDto.getTriggerConditionAval().trim())) {
                    return true;
                }
            }

        }
        if ("3".equals(triggerCondition)) {
            // 3 数值大于A 只考虑数值类型
            Integer triggerConditionValType = triggerDto.getTriggerConditionValType();
            if (triggerConditionValType.intValue() == 0) {
                // 数值类型
                Double alarmDeadZone = CommonUtil.str2Double(triggerDto.getAlarmDeadZone());
                Double num = CommonUtil.str2Double(val);
                double triggerConditionAval = CommonUtil.str2Double(triggerDto.getTriggerConditionAval().trim()).doubleValue();
                if (num.doubleValue() > (triggerConditionAval+alarmDeadZone)) {
                    return true;
                }
            }

        }
        if ("4".equals(triggerCondition)) {
            // 数值小于B
            Integer triggerConditionValType = triggerDto.getTriggerConditionValType();
            if (triggerConditionValType.intValue() == 0) {
                // 数值类型
                Double alarmDeadZone = CommonUtil.str2Double(triggerDto.getAlarmDeadZone());
                Double num = CommonUtil.str2Double(val);
                double triggerConditionBval = CommonUtil.str2Double(triggerDto.getTriggerConditionBval().trim()).doubleValue();
                if (num.doubleValue() > triggerConditionBval - alarmDeadZone) {
                    return true;
                }
            }

        }

        return false;
    }
}
