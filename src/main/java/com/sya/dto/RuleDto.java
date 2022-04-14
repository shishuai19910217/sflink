package com.sya.dto;

import lombok.Data;

import java.io.Serializable;
@Data
public class RuleDto implements Serializable {

    private Integer id;
    private String trigger;
    private String relyRuleId;
    private String triggerType;

    /***
     * 以下是 rule_monitor_element 表
     */
    private String elementType;
    private Integer machineId;
    private Integer uniqueDataPointId;
    private Integer templateId;
    private Integer dataPointId;
    private String sn;
    private Integer operaDataPointId;
}
