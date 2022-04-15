package com.sya.kafka.datapointalarm.rule.process;

import com.alibaba.fastjson.JSONObject;
import com.sya.cache.RuleCacheUtil;
import com.sya.dto.RelyRuleAssociateMapperDto;
import com.sya.dto.RuleBaseDto;
import com.sya.dto.RuleMonitorElement;
import com.sya.kafka.datapointalarm.rule.dto.DataPointDto;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/***
 * 接受【业务模块】缓存通知 动态更新redis缓存
 */
public class DynamicModifyRule implements Serializable {
    /***
     * 规则动态的标签
     */
    private static  final MapStateDescriptor<String, String> stateDescriptor
            = new MapStateDescriptor<>("state", String.class, String.class);

    public static DataStream<DataPointDto> exec(DataStream<DataPointDto> kafkaPointData, StreamExecutionEnvironment env) {
        // 广播流 --规则
        // 为规则打标签
        FlinkKafkaConsumer kafkaRuleModifySource = SourceBuilder.getKafkaRuleModifySource();
        DataStreamSource<String> rules = env.addSource(kafkaRuleModifySource);

        BroadcastStream<String> broadcast = rules.broadcast(stateDescriptor);
        KeyedStream<DataPointDto, String> keyedStream = kafkaPointData.keyBy(new KeySelector<DataPointDto, String>() {
            @Override
            public String getKey(DataPointDto o) throws Exception {
                return o.getSn();
            }
        });
        BroadcastConnectedStream<DataPointDto, String> connect = keyedStream.connect(broadcast);
        // 动态修改规则
        DataStream<DataPointDto> process = connect.process(new KeyedBroadcastProcessFunctionForDynamicModifyRule());
        return process;

    }

    /***
     * 接受到缓存通知 按业务更新redis
     */
    static  class KeyedBroadcastProcessFunctionForDynamicModifyRule extends KeyedBroadcastProcessFunction<DataPointDto, DataPointDto, String, DataPointDto> {
        @Override
        public void processElement(DataPointDto value, ReadOnlyContext ctx, Collector<DataPointDto> out) throws Exception {
            out.collect(value);
        }
        /***
         * @param value
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void processBroadcastElement(String value, Context ctx, Collector<DataPointDto> out) throws Exception {
            /***
             * 接受 规则动态变更 更新redis
             */
            JSONObject pointRule = JSONObject.parseObject(value);
            dynamicModifyRule(pointRule);


        }
        /***
         * {
         *     id:123 ,#######变量id
         *     type: 0,   ######id类型0：代表变量id为unique_data_point_id
         *     ；1：代表变量id为data_point_id；2：代表变量id为opera_data_point_id
         *    action_type:  0   ########操作类型 0:新增；1：修改；2：删除
         *  ruleId:"1,2,3" #######规则id列表
         * }
         *
         * 网关修改/删除  发送 type为0 ，action_type为 2的通知  就是只要变更就删除相关的规则
         * 设备修改/删除  发送 type为0 ，action_type为 2的通知  就是只要变更就删除相关的规则
         * 模板修改/删除  发送 type为0 ，action_type为 2的通知  就是只要变更就删除相关的规则
         * 新增规则 发送action_type为0的通知
         * 修改规则（不会改变数据点） 发送action_type为1的通知
         * 删除规则 发送action_type为2的通知
         * @throws Exception
         */
        private void dynamicModifyRule(JSONObject pointRule ){
            String type = pointRule.getString("type");
            Integer id = pointRule.getInteger("id");
            String actionType = pointRule.getString("actionType");
            String ruleId = pointRule.getString("ruleId");
            String[] split = ruleId.split(",");
            List<Integer> collect = Arrays.stream(split).map(val->{return Integer.parseInt(val);}).collect(Collectors.toList());
            if ("0".equals(type)) {
                if ("2".equals(actionType)) {
                    // 直接删除
                    RuleCacheUtil.delUniqueDataPointRuleMapper(id.toString());
                }else if ("1".equals(actionType)) {
                    // 修改 只更新基础缓存就可以
                    List<RuleBaseDto> ruleBaseDataForDB = RuleCacheUtil.getRuleBaseDataForDB(collect);
                    RuleCacheUtil.setRuleBaseData(ruleBaseDataForDB);
                }else if ("0".equals(actionType)) {
                    // 新增 也可能是追加
                    List<String> uniqueDataPointRuleMapper = RuleCacheUtil.getUniqueDataPointRuleMapper(id.toString());
                    List<String> newData = collect.stream().map(val -> {
                        return val.toString();
                    }).collect(Collectors.toList());
                    if (CommonUtil.judgeEmpty(uniqueDataPointRuleMapper)) {
                        uniqueDataPointRuleMapper = newData;
                    }else {
                        uniqueDataPointRuleMapper.addAll(newData);
                    }
                    // 追加映射
                    RuleCacheUtil.setUniqueDataPointRuleMapper(id.toString(),String.join(",",uniqueDataPointRuleMapper));
                    // 新增基本缓存
                    List<RuleBaseDto> ruleBaseDataForDB = RuleCacheUtil.getRuleBaseDataForDB(collect);
                    RuleCacheUtil.setRuleBaseData(ruleBaseDataForDB);
                }
            }else if ("1".equals(type)) {
                //dataPointId
                if ("2".equals(actionType)) {
                    // 直接删除
                    RuleCacheUtil.delDataPointRuleMapper(id.toString());

                }else if ("1".equals(actionType)) {
                    // 修改 只更新基础缓存就可以
                    List<RuleBaseDto> ruleBaseDataForDB = RuleCacheUtil.getRuleBaseDataForDB(collect);
                    RuleCacheUtil.setRuleBaseData(ruleBaseDataForDB);
                }else if ("0".equals(actionType)) {
                    // 新增 也可能是追加
                    List<String> uniqueDataPointRuleMapper = RuleCacheUtil.getDataPointRuleMapper(id.toString());
                    List<String> newData = collect.stream().map(val -> {
                        return val.toString();
                    }).collect(Collectors.toList());
                    if (CommonUtil.judgeEmpty(uniqueDataPointRuleMapper)) {

                        uniqueDataPointRuleMapper = newData;
                    }else {
                        uniqueDataPointRuleMapper.addAll(newData);
                    }
                    // 追加映射
                    RuleCacheUtil.setDataPointRuleMapper(id.toString(),String.join(",",uniqueDataPointRuleMapper));
                    // 新增基本缓存
                    List<RuleBaseDto> ruleBaseDataForDB = RuleCacheUtil.getRuleBaseDataForDB(collect);
                    RuleCacheUtil.setRuleBaseData(ruleBaseDataForDB);
                }
            }else if ("2".equals(type)) {
                //operaDataPointId
                if ("2".equals(actionType)) {
                    // 直接删除
                    RuleCacheUtil.delOperaDataPointRuleMapper(id.toString());

                }else if ("1".equals(actionType)) {
                    // 修改 只更新基础缓存就可以
                    List<RuleBaseDto> ruleBaseDataForDB = RuleCacheUtil.getRuleBaseDataForDB(collect);
                    RuleCacheUtil.setRuleBaseData(ruleBaseDataForDB);
                }else if ("0".equals(actionType)) {
                    // 新增 也可能是追加
                    List<String> uniqueDataPointRuleMapper = RuleCacheUtil.getOperaDataPointRuleMapper(id.toString());
                    List<String> newData = collect.stream().map(val -> {
                        return val.toString();
                    }).collect(Collectors.toList());
                    if (CommonUtil.judgeEmpty(uniqueDataPointRuleMapper)) {

                        uniqueDataPointRuleMapper = newData;
                    }else {
                        uniqueDataPointRuleMapper.addAll(newData);
                    }
                    // 追加映射
                    RuleCacheUtil.setOperaDataPointRuleMapper(id.toString(),String.join(",",uniqueDataPointRuleMapper));
                    // 新增基本缓存
                    List<RuleBaseDto> ruleBaseDataForDB = RuleCacheUtil.getRuleBaseDataForDB(collect);
                    RuleCacheUtil.setRuleBaseData(ruleBaseDataForDB);
                }
            }
        }
    }


}
