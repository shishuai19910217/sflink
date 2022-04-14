package com.sya.cache;

import com.alibaba.fastjson.JSONObject;
import com.sya.config.MybatisConfig;
import com.sya.constants.RuleCacheConstant;
import com.sya.dto.*;
import com.sya.kafka.datapointalarm.rule.dto.DevicePointRelDto;
import com.sya.mapper.DeviceTemplatePointRelMapper;
import com.sya.mapper.RuleMapper;
import com.sya.mapper.RuleMonitorElementMapper;
import com.sya.utils.CommonUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/****
 * 获取 规则缓存
 */
@Slf4j
public class DeviceCacheUtil {

    public static List<DevicePointRelDto> getDevicePointRelsBySnForDB(List<String> sns){
        DeviceTemplatePointRelMapper mapper = MybatisConfig.getMapper(DeviceTemplatePointRelMapper.class);
        return mapper.getList(sns);
    }

    public static void delDevicePointRel(List<String> snList) {
        RedisUtil.delDevicePointRel(snList);
    }

    public static void setDevicePointRel(Map<String,List<DevicePointRelDto>> map) {
        RedisUtil.setDevicePointRel(map);
    }
    public static String getDevicePointRel(String sn,String dataPointId) {
        return RedisUtil.getDevicePointRel(sn,dataPointId);
    }

}
