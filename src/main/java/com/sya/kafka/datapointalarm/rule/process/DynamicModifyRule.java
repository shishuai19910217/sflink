package com.sya.kafka.datapointalarm.rule.process;

import com.alibaba.fastjson.JSONObject;
import com.sya.cache.RedisUtil;
import com.sya.cache.RuleCacheUtil;
import com.sya.dto.RuleBaseCacheDto;
import com.sya.dto.RuleBaseDto;
import com.sya.dto.RuleMonitorElement;
import com.sya.kafka.datapointalarm.rule.dto.DataPointDto;
import com.sya.kafka.datapointalarm.rule.source.SourceBuilder;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.List;
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
         * {
         *     id:123 ,#######变量id
         *     type: 0,   ######id类型0：代表变量id为unique_data_point_id  ；1：代表变量id为data_point_id；2：代表变量id为opera_data_point_id
         *    action_type:  0   ########操作类型0:新增；1：修改；2：删除
         *  ruleId:"1,2,3" #######规则id列表
         * }
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
         *     type: 0,   ######id类型0：代表变量id为unique_data_point_id  ；1：代表变量id为data_point_id；2：代表变量id为opera_data_point_id
         *    action_type:  0   ########操作类型0:新增；1：修改；2：删除
         *  ruleId:"1,2,3" #######规则id列表
         * }
         * @throws Exception
         */
        private void dynamicModifyRule(JSONObject pointRule ){
            String type = pointRule.getString("type");
            Integer id = pointRule.getInteger("id");
            String actionType = pointRule.getString("actionType");
            if ("0".equals(type)) {
                // 获取 mysql 数据
                List<RuleMonitorElement> list = RuleCacheUtil.getRuleMonitorElementForDB(id);
                if (null == list || list.size() <= 0) {
                    return;
                }
                String ruleIdStr = list.stream().map(action -> {
                    return action.getRuleId().toString();
                }).collect(Collectors.joining(","));
                String sn = list.get(0).getSn();
                List<Integer> ruleIds = list.stream().map(action -> {
                    return action.getRuleId();
                }).collect(Collectors.toList());
                // 更新映射关系
                RuleCacheUtil.setUniqueDataPointRuleMapper(id.toString(),ruleIdStr);
                // 获取 当前数据点所有的
                List<RuleBaseDto> ruleBaseDataForDB = RuleCacheUtil.getRuleBaseDataForDB(ruleIds);
                // 更新缓存
                RuleCacheUtil.setRuleBaseData(ruleBaseDataForDB);
            }
        }
    }


}
