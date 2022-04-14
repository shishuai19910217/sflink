package com.sya.dto;

import lombok.Data;

import java.io.Serializable;
@Data
public class SnAlaram implements Serializable {
    /**
     * sn
     */
    private String sn;
    /***
     * 规则id
     */
    private Integer ruleId;
    /***
     * 报警详情
     */
    private String msg;
    /***
     * 产生时间
     */
    private long time;
    // 1 恢复  0 报警
    private String status ;
    /***
     * 报警类型  1
     */
    private String alarmType;


    public SnAlaram(String sn, Integer ruleId, String msg, long time) {
        this.sn = sn;
        this.ruleId = ruleId;
        this.msg = msg;
        this.time = time;
    }

    public SnAlaram(String sn, Integer ruleId, String msg, long time, String status) {
        this.sn = sn;
        this.ruleId = ruleId;
        this.msg = msg;
        this.time = time;
        this.status = status;
    }

    public SnAlaram(){}


    @Override
    public String toString() {
        return "SnAlaram{" +
                "sn='" + sn + '\'' +
                ", ruleId=" + ruleId +
                ", msg='" + msg + '\'' +
                ", time=" + time +
                ", status='" + status + '\'' +
                '}';
    }
}
