package com.sya.kafka.datapointalarm;

import lombok.Data;

import java.io.Serializable;
@Data
public class DatapointAlarmPushMessageDto implements Serializable {
    private String uniqueDataPointId;
    private String sn;
    private String deviceId;
    private String msg;
    private String rule;

    public DatapointAlarmPushMessageDto(String uniqueDataPointId, String sn, String deviceId, String msg, String rule) {
        this.uniqueDataPointId = uniqueDataPointId;
        this.sn = sn;
        this.deviceId = deviceId;
        this.msg = msg;
        this.rule = rule;
    }
}
