package com.sya.kafka.datapointalarm.rule.process;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.sya.cache.CacheUtil;
import com.sya.cache.DeviceCacheUtil;
import com.sya.cache.RuleCacheUtil;
import com.sya.dto.RuleBaseDto;
import com.sya.dto.RuleMonitorElement;
import com.sya.kafka.datapointalarm.rule.dto.DataPointDto;
import com.sya.kafka.datapointalarm.rule.dto.DevicePointRelDto;
import com.sya.kafka.datapointalarm.rule.source.SourceBuilder;
import com.sya.utils.CommonUtil;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/***
 * 接受【业务模块】缓存通知 动态更新redis中sn与数据点的映射
 */
public class DynamicModifyDeviceUniqueDataPointIdMapper implements Serializable {

    /***
     * 触发时机
     * 删除网关  --删除缓存就行
     * 删除模板  --删除缓存就行
     * 删除从机  --覆盖或者删除
     * 删除变量  --覆盖或者删除
     * 添加变量  --覆盖
     * 修改网关-模板变更  --覆盖
     * @param env
     */
    public static void exec(StreamExecutionEnvironment env) {
        DataStreamSource<JSONObject> dataStreamSource = env.addSource(SourceBuilder.getKafkaDeviceUniqueDataPointIdMapperModifySource());
        dataStreamSource.map(action->{
            JSONArray sns = action.getJSONArray("sn");
            List<String> snList = new ArrayList<>();
            for (int i = 0; i < sns.size(); i++) {
                String sn = sns.get(i).toString();
                snList.add(sn);
            }

            List<DevicePointRelDto> devicePointRelsBySnForDB = DeviceCacheUtil.getDevicePointRelsBySnForDB(snList);

            if (CommonUtil.judgeEmpty(devicePointRelsBySnForDB)) {
               // 就删除所有相关缓存sns
                DeviceCacheUtil.delDevicePointRel(snList);
            }else {

                // 数据库存在的需要重新覆盖缓存
                Map<String, List<DevicePointRelDto>> mapForDb = devicePointRelsBySnForDB.stream().collect(Collectors.groupingBy(key -> {
                    return key.getSn();
                }));
                DeviceCacheUtil.setDevicePointRel(mapForDb);
                // 数据库不存在的删除
                List<String> delCacheList = new ArrayList<>();
                for (String s : snList) {
                    if (!mapForDb.containsKey(s)) {
                        delCacheList.add(s);
                    }
                }
                if (CommonUtil.judgeEmpty(delCacheList)) {
                    DeviceCacheUtil.delDevicePointRel(delCacheList);
                }

            }


            return action;
        });
    }



}
