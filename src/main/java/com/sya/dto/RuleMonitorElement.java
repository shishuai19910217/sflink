package com.sya.dto;

import lombok.Data;

import java.io.Serializable;
@Data
public class RuleMonitorElement implements Serializable {
    private Integer id;
    private Integer ruleId;
    private String elementType;
    private Integer machineId;
    private Integer uniqueDataPointId;
    private Integer templateId;
    private Integer dataPointId;
    private String sn;
    private Integer operaDataPointId;
    public RuleMonitorElement(){}
    public RuleMonitorElement(Integer ruleId, String elementType, Integer machineId, Integer uniqueDataPointId,
                              Integer templateId, Integer dataPointId, String sn, Integer operaDataPointId) {
        this.ruleId = ruleId;
        this.elementType = elementType;
        this.machineId = machineId;
        this.uniqueDataPointId = uniqueDataPointId;
        this.templateId = templateId;
        this.dataPointId = dataPointId;
        this.sn = sn;
        this.operaDataPointId = operaDataPointId;
    }
}
