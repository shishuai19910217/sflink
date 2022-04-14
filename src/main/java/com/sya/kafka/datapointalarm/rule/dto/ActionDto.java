package com.sya.kafka.datapointalarm.rule.dto;

import lombok.Data;

import java.util.List;

/**
 * @Author zhanwu
 * @Date 2022/4/11 17:49
 * @Description
 */
@Data
public class ActionDto {
    /**
     * 推送间隔  单位秒
     */
    private Integer pushInterval;

    /**
     * 推送频率  0 仅推送1次  1 间隔推送
     */
    private Integer pushFrequency;

    /**
     * 推送方式 以","开始结束 ,1,2,
     */
    private String pushWay;

    /**
     * 控制对象类型（1 设备 2 网关  3模板      ）
     */
    private Integer controlObjType;

    /**
     * 控制对象（设备id/网关sn/变量模板id）
     */
    private String controlObj;

    /**
     * 控制下发动作列表
     */
    private List<ControlActionDto> controlActionDtos;
}
