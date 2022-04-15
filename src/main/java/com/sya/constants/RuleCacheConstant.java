package com.sya.constants;

public final class RuleCacheConstant {
    /***
     * 规则缓存key的前缀
     *
     */
    public static final String RULE_BASE_CACHE_PREFIX = "rulebase-";

    /***
     * uniqueDataPointId对应的规则id映射缓存
     *
     */
    public static final String RULE_UNIQUEDATAPOINT_CACHE_PREFIX = "ruleuniqueDataPoint-";

    /***
     * DataPointId对应的规则id映射缓存
     *
     */
    public static final String RULE_DATAPOINT_CACHE_PREFIX = "ruleDataPoint-";

    /***
     * operaDataPointId对应的规则id映射缓存
     *
     */
    public static final String RULE_OPERADATAPOINT_CACHE_PREFIX = "ruleoperaDataPoint-";
    /***
     * 缓存数据点最新的报警信息 方便
     */
    public static final  String LAST_DATAPOINTALARMDATA_PREFIX = "lastdatapointalarmdata-";

    /***
     * 某个数据点某个规则是否已推送
     */
    public static final  String DATAPOINTALARMPUSH_PREFIX = "datapointalarmpushsttus-";

    /***
     * 报警规则关联的控制规则
     */
    public static final String ALARM_RELY_RULEIDS_PREFIX = "alarmRelyRuleIds-";

    /***
     * 网关与UniqueDataPointId的映射
     */
    public static final String DEVICE_DATAPOINT_PREFIX = "devicedatapoint-";

    /***
     * 网关与machine的映射
     */
    public static final String DEVICE_MACHINE_PREFIX = "devicemachine-";

    /***
     * 报警规则下相关链的控制规则
     */
    public static final String ALARMCONTROL_PREFIX = "alarmcontrol-";
}
