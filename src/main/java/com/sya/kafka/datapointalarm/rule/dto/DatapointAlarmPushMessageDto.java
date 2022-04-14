package com.sya.kafka.datapointalarm.rule.dto;

import com.sya.dto.RuleBaseDto;
import lombok.Data;

import java.io.Serializable;

@Data
public class DatapointAlarmPushMessageDto implements Serializable {
    /***
     * 报警的数据点
     */
    private String uniqueDataPointId;
    /***
     * 报警的网关
     */
    private String sn;
    /***
     * 报警的设备
     */
    private String deviceId;
    /***
     * 当时的报警对象
     */
    private RuleBaseDto rule;

    public DatapointAlarmPushMessageDto(String uniqueDataPointId, String sn, String deviceId, RuleBaseDto rule) {
        this.uniqueDataPointId = uniqueDataPointId;
        this.sn = sn;
        this.deviceId = deviceId;
        this.rule = rule;
    }
}
