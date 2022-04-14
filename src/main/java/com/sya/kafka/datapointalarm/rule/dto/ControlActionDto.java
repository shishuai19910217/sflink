package com.sya.kafka.datapointalarm.rule.dto;

import lombok.Data;

import java.util.Date;

/**
 * @Author zhanwu
 * @Date 2022/4/12 10:19
 * @Description
 */
@Data
public class ControlActionDto {
    /**
     * 控制类型（0 采集  1 控制）
     */
    private Integer controlType;

    /**
     * 设备的计算变量变量id
     */
    private Integer controlOperaDataPoint;

    /**
     * 控制模板变量id
     */
    private Integer controlDataPointId;
    /**
     * 控制网关变量id
     */
    private Integer controlUniqueDataPointId;

    /***
     * 控制对象id  报警触发此字段为空
     */
    private Integer controlObj;

    /***
     * 控制对象类型 报警触发此字段为空
     */
    private Integer controlObjType ;

    /**
     * 下发数据
     */
    private String controlData;

    /**
     * 执行时间-年
     */
    private Integer executeYear;

    /**
     * 执行时间-月
     */
    private Integer executeMonth;

    /**
     * 执行时间-日
     */
    private Integer executeDay;

    /**
     * 执行时间-周
     */
    private Integer executeWeek;

    /**
     * 执行时间-时
     */
    private Integer executeHour;

    /**
     * 执行时间-分
     */
    private Integer executeMinute;

    /**
     * 执行时间-秒
     */
    private Integer executeSecond;

    /**
     * 执行间隔(分钟)
     */
    private Integer executeInterval;

    /**
     * 开始日期
     */
    private Date startDt;
    /**
     * 结束日期
     */
    private Date endDt;
    /**
     * cron表达式
     */
    private String cron;
}
