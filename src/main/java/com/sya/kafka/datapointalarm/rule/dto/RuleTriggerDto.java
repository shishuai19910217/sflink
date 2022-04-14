package com.sya.kafka.datapointalarm.rule.dto;

import lombok.Data;

import java.io.Serializable;
@Data
public class RuleTriggerDto implements Serializable {
    /**
     * 触发条件  0 开关ON 1 开关OFF2 数值等于A3 数值大于A4 数值小于B5 数值大于A且小于B6.数值小于A或者大于B
     */
    private String triggerCondition;

    /**
     * 数值A
     */
    private String triggerConditionAval;
    /**
     * 数值B
     */
    private String triggerConditionBval;

    /**
     * 值类型  0数值  1字符串  默认0
     */
    private Integer triggerConditionValType;

    /**
     * 报警信息
     */
    private String alarmMsg;

    /**
     * 延时时间 秒
     */
    private Integer delayTime;

    /**
     * 死区
     */
    private String alarmDeadZone;
}
