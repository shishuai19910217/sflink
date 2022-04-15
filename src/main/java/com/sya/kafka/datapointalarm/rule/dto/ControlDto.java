package com.sya.kafka.datapointalarm.rule.dto;

import com.sya.utils.CommonUtil;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
public class ControlDto implements Serializable {
    /***
     * 记录id
     */
    private String controlUuid;
    /***
     * 关联的报警记录
     */
    private String alarmUuid;


    /***
     * 控制设备
     */
    private List<Integer> controlMachineIds;

    /***
     * 控制网关
     */
    private String controlSn;

    /***
     * 数据点id
     */
    private Integer uniqueDataPointId;

    /***
     * 计算性数据点id
     */
    private Integer operaDataPointId;

    /***
     * 控制内容
     */
    private String controlData;

    /**
     * 控制类型（0 采集  1 控制）
     */
    private Integer controlType;

    /***
     * 触发条件
     */
    private String trigger;
    public ControlDto(){}
    public ControlDto(String controlUuid, String alarmUuid,  String controlData, Integer controlType, String trigger) {
        this.controlUuid = controlUuid;
        this.alarmUuid = alarmUuid;
        this.controlData = controlData;
        this.controlType = controlType;
        this.trigger = trigger;
    }

    public void addControlMachineId(Integer controlmachineId){
        if (CommonUtil.judgeEmpty(this.controlMachineIds)) {
            this.controlMachineIds = new ArrayList<>();
        }
        this.controlMachineIds.add(controlmachineId);
    }
}
