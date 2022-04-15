package com.sya.cache;

import com.alibaba.fastjson.JSONObject;
import com.sya.config.MybatisConfig;
import com.sya.constants.RuleCacheConstant;
import com.sya.dto.*;
import com.sya.kafka.datapointalarm.rule.dto.DevicePointRelDto;
import com.sya.kafka.datapointalarm.rule.dto.MachineDeviceRelDto;
import com.sya.mapper.DeviceTemplatePointRelMapper;
import com.sya.mapper.MachineDeviceRelMapper;
import com.sya.mapper.RuleMapper;
import com.sya.mapper.RuleMonitorElementMapper;
import com.sya.utils.CommonUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
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

    public static List<MachineDeviceRelDto> getMachineDeviceRelsBySnForDB(List<String> sns){
        MachineDeviceRelMapper mapper = MybatisConfig.getMapper(MachineDeviceRelMapper.class);
        return  mapper.getList(sns);

    }

    public static List<MachineDeviceRelDto> getMachineDeviceRelsBySnForDB( String  sn ){
        MachineDeviceRelMapper mapper = MybatisConfig.getMapper(MachineDeviceRelMapper.class);
        List<String> sns = new ArrayList<>();
        sns.add(sn);
        return  mapper.getList(sns);

    }
    public static void delDeviceMachineMapper(List<String> sns) {
        List<String> collect = sns.stream().map(action -> {
            return RuleCacheConstant.DEVICE_MACHINE_PREFIX + action;
        }).collect(Collectors.toList());
        CacheUtil.del(collect);
    }

    public static void setDeviceMachineMapper(Map<String, List<MachineDeviceRelDto>> unbindMap) {
        Set<String> sns = unbindMap.keySet();
        Map<String,Object> map = new HashMap<>(16);
        for (String sn : sns) {
            String collect = unbindMap.get(sn).stream().map(action -> {
                return action.getMachineId().toString();
            }).collect(Collectors.joining());
            map.put(RuleCacheConstant.DEVICE_MACHINE_PREFIX+sn,collect);

        }
        CacheUtil.set(map);

    }

    public static String getDeviceMachineMapper(String sn) {
        String str = CacheUtil.getStr(RuleCacheConstant.DEVICE_MACHINE_PREFIX + sn);
        if (CommonUtil.judgeEmpty(str)) {
            List<MachineDeviceRelDto> machineDeviceRelsBySnForDB = getMachineDeviceRelsBySnForDB(sn);
            Map<String, List<MachineDeviceRelDto>> map = machineDeviceRelsBySnForDB.stream().collect(Collectors.groupingBy(key -> {
                return key.getDeviceSn();
            }));
            setDeviceMachineMapper(map);
            str = CacheUtil.getStr(RuleCacheConstant.DEVICE_MACHINE_PREFIX + sn);
        }
        return str;
    }
}
