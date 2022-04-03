package com.sya.kafka.datapointalarm;

import lombok.Data;

import java.io.Serializable;

@Data
public class AlarmRuleMonitorElementDto implements Serializable {
    private Integer alarmDatapointRuleId;
    private String sn;
    private Integer uniqueDataPointId;
    private Integer templateId;
    private Integer dataPointId;



}
