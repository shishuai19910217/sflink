package com.sya.cache;

import com.alibaba.fastjson.JSONObject;
import com.sya.config.MybatisConfig;
import com.sya.constants.RuleCacheConstant;
import com.sya.dto.*;
import com.sya.mapper.RuleMapper;
import com.sya.mapper.RuleMonitorElementMapper;
import com.sya.utils.CommonUtil;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/****
 * 获取 规则缓存
 */
@Slf4j
public class PointDataCacheUtil {


    /***
     * 是否存在上次报警
     */
    public static Boolean existsLastAlarmStatus(String ruleId,String uniqueDataPointId ){
        return RedisUtil.existsLastAlarmStatus(ruleId,uniqueDataPointId);
    }

    /***
     * 保存最新的报警状态
     */
    public static Boolean setLastAlarmStatus(String ruleId,String uniqueDataPointId,String data ){

        return RedisUtil.setLastAlarmStatus(ruleId,uniqueDataPointId,data);
    }

    /***
     *  删除最新的报警状态
     */
    public static void delLastAlarmStatus(String ruleId,String uniqueDataPointId ){
        RedisUtil.delLastAlarmStatus(ruleId,uniqueDataPointId);
    }

    /***
     * 某个规则是不是推送过了
     */
    public static Boolean existsDatapointAlaramPush(String ruleId,String uniqueDataPointId ){
        return RedisUtil.existsDatapointAlaramPush(ruleId,uniqueDataPointId);
    }
    /***
     * 某个规则是不是推送过了
     */
    public static void setDatapointAlaramPushStatus(String ruleId,String uniqueDataPointId ){
        RedisUtil.setDatapointAlaramPushStatus(ruleId,uniqueDataPointId);
    }
    /***
     * 某个规则是不是推送过了
     */
    public static void delDatapointAlaramPushStatus(String ruleId,String uniqueDataPointId){

          RedisUtil.delDatapointAlaramPushStatus(ruleId,uniqueDataPointId);
    }
}
