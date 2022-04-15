package com.sya.kafka.datapointalarm.rule.process;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.sya.cache.DeviceCacheUtil;
import com.sya.kafka.datapointalarm.rule.dto.DevicePointRelDto;
import com.sya.kafka.datapointalarm.rule.dto.MachineDeviceRelDto;
import com.sya.kafka.datapointalarm.rule.source.SourceBuilder;
import com.sya.utils.CommonUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/***
 * 接受【业务模块】缓存通知 动态更新redis中machine 与device的映射关系
 */
public class DynamicModifyDeviceMachineMapper implements Serializable {

    /***
     触发时机
     设备关联网关时 （修改 ---可能是解绑）
     * @param env
     */
    public static void exec(StreamExecutionEnvironment env) {
        DataStreamSource<JSONObject> dataStreamSource = env.addSource(SourceBuilder.getKafkaDeviceMachineMapperModifySource());
        dataStreamSource.map(action->{

            // 这是需要 解绑的
            String snUnbind = action.getString("snUnbind");
            if (!CommonUtil.judgeEmpty(snUnbind)) {
                String[] split = snUnbind.split(",");
                List<String> snUnbindList = Arrays.stream(split).collect(Collectors.toList());
                List<MachineDeviceRelDto> machineDeviceRelsBySnForDB = DeviceCacheUtil.getMachineDeviceRelsBySnForDB(snUnbindList);
                if (CommonUtil.judgeEmpty(machineDeviceRelsBySnForDB)) {
                    DeviceCacheUtil.delDeviceMachineMapper(snUnbindList);
                }else {
                    // 数据库存在的就覆盖
                    Map<String, List<MachineDeviceRelDto>> unbindMap = machineDeviceRelsBySnForDB.stream().collect(Collectors.groupingBy(key -> {
                        return key.getDeviceSn();
                    }));
                    DeviceCacheUtil.setDeviceMachineMapper(unbindMap);
                    // 数据库不存在的就删除
                    List<String> delCache = new ArrayList<>();
                    for (String s : snUnbindList) {
                        if (!unbindMap.containsKey(s)) {
                            delCache.add(s);

                        }
                    }
                    if (!CommonUtil.judgeEmpty(delCache)) {
                        DeviceCacheUtil.delDeviceMachineMapper(delCache);
                    }
                }

            }


            // 这是需要绑定的
            String snBind = action.getString("snBind");
            if (!CommonUtil.judgeEmpty(snBind)) {
                String[] split = snBind.split(",");
                List<String>  bindList = Arrays.stream(split).collect(Collectors.toList());
                List<MachineDeviceRelDto> machineDeviceRelsBySnForDB = DeviceCacheUtil.getMachineDeviceRelsBySnForDB(bindList);
                if (!CommonUtil.judgeEmpty(machineDeviceRelsBySnForDB)) {
                    // 数据库存在的就覆盖
                    Map<String, List<MachineDeviceRelDto>> bindMap = machineDeviceRelsBySnForDB.stream().collect(Collectors.groupingBy(key -> {
                        return key.getDeviceSn();
                    }));
                    DeviceCacheUtil.setDeviceMachineMapper(bindMap);
                }


            }

            return action;
        });


    }



}
