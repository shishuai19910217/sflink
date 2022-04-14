package com.sya.kafka.datapointalarm;

import lombok.Data;

import java.io.Serializable;

@Data
public class DataPointDto implements Serializable {
    private String sn;
    private Integer datapointId;
    private Integer uniqueDataPointId;
    private Integer templateId;
    private String val;
    private String alarmRuleIdFormonitorDev;
    private String alarmRuleIdFormonitorTemlate;
    private String ruleId;
    private String msg;
    private Integer deviceId;
    /***
     * 是否是恢复流
     */
    private Boolean recover = false;
    /***
     * 是否需要真正的最终入库
     */
    private Boolean storage = true;
    public DataPointDto(){}
    public DataPointDto(String sn, Integer datapointId, Integer uniqueDataPointId, Integer templateId, String val) {
        this.sn = sn;
        this.datapointId = datapointId;
        this.uniqueDataPointId = uniqueDataPointId;
        this.templateId = templateId;
        this.val = val;
    }
}
