package com.sya.kafka.datapointalarm;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class AlarmRuleDto implements Serializable {
    private Integer id;
    // 0 数采设备  1 变量模板 默认时0
    private String monitorObj;
    /**
     * 触发条件  0 开关ON  1 开关OFF  2 数值等于A  3 数值大于A  4 数值小于B  5 数值大于A且小于B  6.数值小于A或者大于B',
      */ 
    private String triggerCondition;
    /***
     * 触发条件数值A的值
     */
    private String triggerConditionAVal;

    /***
     * 触发条件数值B的值
     */
    private String triggerConditionBVal;

    /***
     * 触发数据的类型0 数值  1 字符串
     *
     */
    private String triggerConditionValType;
    // 报警的推送信息
    private String alarmMsg;
    /***
     * 报警死区
     */
    private String alarmDeadZone;
    /***
     * 延时时长（秒） 默认是0
     */
    private int delayTime;

    /***
     * 是否推送 0 不推送 1 推送
     */
    private String pushStatus;
    /***
     * 推送方式 以","开始结束 ,1,2,
     */
    private String pushWay;
    /***
     * 0 仅推送1次  1 间隔推送
      */
    private String  pushFrequency;
    /***
     * 推送间隔
     */
    private int pushInterval;
    private String monitorElement;
    private List<AlarmRuleMonitorElementDto> elementList;


}
