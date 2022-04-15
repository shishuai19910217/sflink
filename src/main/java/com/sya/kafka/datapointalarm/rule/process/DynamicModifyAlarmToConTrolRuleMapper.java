package com.sya.kafka.datapointalarm.rule.process;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.sya.cache.DeviceCacheUtil;
import com.sya.cache.RuleCacheUtil;
import com.sya.dto.RelyRuleAssociateMapperDto;
import com.sya.kafka.datapointalarm.rule.dto.MachineDeviceRelDto;
import com.sya.kafka.datapointalarm.rule.source.SourceBuilder;
import com.sya.utils.CommonUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/***
 * 接受【业务模块】缓存通知 动态更新redis中报警规则与之相关的控制规则的映射
 */
public class DynamicModifyAlarmToConTrolRuleMapper implements Serializable {


    public static void exec(StreamExecutionEnvironment env) {
        DataStreamSource<JSONObject> dataStreamSource = env.addSource(SourceBuilder.getKafkaAlarmToControlMapperModifySource());
        dataStreamSource.map(action->{
            JSONArray alarmRuleId = action.getJSONArray("alarmRuleId");
            List<Integer> alarmIds = new ArrayList<>();
            for (Object o : alarmRuleId) {
                int alarmId = Integer.parseInt(o.toString());
                alarmIds.add(alarmId);
            }
            List<RelyRuleAssociateMapperDto> ruleBaseDataByRelyRuleIdForDB = RuleCacheUtil.getRuleBaseDataByRelyRuleIdForDB(alarmIds);
            Map<String, List<RelyRuleAssociateMapperDto>> map = ruleBaseDataByRelyRuleIdForDB.stream().collect(Collectors.groupingBy(key -> {
                return key.getAlarmId().toString();
            }));
            Set<String> alarmIdsForDB = map.keySet();
            // 数据库中没有的删除
            List<Integer> delList = new ArrayList<>();
            for (Integer alarmId : alarmIds) {
                if (!map.containsKey(alarmId.toString())) {
                    delList.add(alarmId);
                }
            }
            if (!CommonUtil.judgeEmpty(delList)) {
                RuleCacheUtil.delAlarmControlMapper(delList);
            }
            // 数据库同步至缓存
            RuleCacheUtil.setAlarmControlMapper(map);

            return action;
        });


    }



}
