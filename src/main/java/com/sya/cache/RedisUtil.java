package com.sya.cache;

import com.alibaba.fastjson.JSONObject;
import com.sya.config.MybatisConfig;
import com.sya.constants.RuleCacheConstant;
import com.sya.kafka.datapointalarm.AlarmRuleDto;
import com.sya.kafka.datapointalarm.AlarmRuleMonitorElementDto;
import com.sya.kafka.datapointalarm.rule.dto.DevicePointRelDto;
import com.sya.mapper.DeviceTemplatePointRelMapper;
import com.sya.utils.CommonUtil;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;


public class RedisUtil {
    public static final GenericObjectPool<StatefulRedisConnection<String, String>> pool;

    static {

        RedisURI redisUri = RedisURI.Builder.
                redis("127.0.0.1",6379)
                .withDatabase(2).build();
      //  redisUri.setPassword("nuojiehongkao2022".toCharArray());
        GenericObjectPoolConfig<StatefulRedisConnection<String, String>> poolConfig = new GenericObjectPoolConfig();
        RedisClient client = RedisClient.create(redisUri);
        pool = ConnectionPoolSupport.createGenericObjectPool(() -> {
                    return client.connect();
                }, poolConfig);


    }


    public static void setStr (String key,String val){
        StatefulRedisConnection<String, String> redisConnection = null;
        try {
            redisConnection = pool.borrowObject();
            RedisCommands<String, String> sync = redisConnection.sync();
            sync.set(key,val);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if (null != redisConnection) {
                pool.returnObject(redisConnection);
            }
        }
    }
    public static void setHash (String key,String filed ,String val){
        StatefulRedisConnection<String, String> redisConnection = null;
        try {
            redisConnection = pool.borrowObject();
            RedisCommands<String, String> sync = redisConnection.sync();
            sync.hset(key,filed,val);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if (null != redisConnection) {
                pool.returnObject(redisConnection);
            }
        }
    }

    /***
     * ?????? ????????????????????????????????????
     * @param sn
     */
    public static List<String> getDeviceModelAlaramRuleIds(String sn, String uniqueDataPointId ){
        StatefulRedisConnection<String, String> redisConnection = null;
        List<String> ruleIdList = new ArrayList<>();
        try {
            redisConnection = pool.borrowObject();
            RedisCommands<String, String> sync = redisConnection.sync();
            String ruleIdStr = sync.hget("ad_"+sn, uniqueDataPointId);
            String[] ruleIds = ruleIdStr.split(",");
            for (String ruleId : ruleIds) {
                ruleIdList.add(ruleId);
            }
            return ruleIdList;
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if (null != redisConnection) {
                pool.returnObject(redisConnection);
            }
        }
        return null;
    }

    /***
     *
     * @param sn
     * @return
     * key ???uniqueDataPointId  val??? ?????????????????????????????????id
     * 123:"1,2,3", // 123 ??????unique_data_point_id ???1,2,3 ?????????????????????????????????id
     */
    public static Map<String, String> getDeviceModelAlaramRuleIds(String sn ){
        StatefulRedisConnection<String, String> redisConnection = null;
        try {
            redisConnection = pool.borrowObject();
            RedisCommands<String, String> sync = redisConnection.sync();
            Map<String, String> hgetall = sync.hgetall("ad_" + sn);
            return hgetall;
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if (null != redisConnection) {
                pool.returnObject(redisConnection);
            }
        }
        return null;
    }

    /***
     * ?????? ??????????????????
     */
    public static List<AlarmRuleDto> getAlarmRuleData(List<String> alarmIds ){
        StatefulRedisConnection<String, String> redisConnection = null;
        try {
            redisConnection = pool.borrowObject();
            RedisCommands<String, String> sync = redisConnection.sync();
            String[] alarmIdArr = alarmIds.toArray(new String[0]);
            List<KeyValue<String, String>> mget = sync.mget(alarmIdArr);


        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if (null != redisConnection) {
                pool.returnObject(redisConnection);
            }
        }
        return null;
    }

    /***
     * ?????? ??????????????????
     */
    public static AlarmRuleDto getAlarmRuleData(String alarmId ){
        StatefulRedisConnection<String, String> redisConnection = null;
        try {
            redisConnection = pool.borrowObject();
            RedisCommands<String, String> sync = redisConnection.sync();
            String data = sync.get("a_" + alarmId);
            AlarmRuleDto dto = JSONObject.parseObject(data, AlarmRuleDto.class);
            String monitorElement = dto.getMonitorElement();
            //device_id sn unique_data_point_id template_id data_point_id
            String[] split = monitorElement.split("#");
            List<AlarmRuleMonitorElementDto> list = new ArrayList<>();
            for (String s : split) {
                AlarmRuleMonitorElementDto bean = new AlarmRuleMonitorElementDto();

                String[] split1 = s.split("-");
                String sn = split1[1];
                String uniqueDataPointId = split1[2];
                if (!"null".equals(uniqueDataPointId)) {
                    bean.setUniqueDataPointId(Integer.parseInt(uniqueDataPointId));
                }
                String templateId = split1[3];
                if (!"null".equals(templateId)) {
                    bean.setTemplateId(Integer.parseInt(templateId));
                }

                String dataPointId = split1[4];
                if (!"null".equals(dataPointId)) {
                    bean.setDataPointId(Integer.parseInt(dataPointId));
                }
                bean.setSn(s);
                list.add(bean);

            }
            dto.setElementList(list);
            return dto;

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if (null != redisConnection) {
                pool.returnObject(redisConnection);
            }
        }
        return null;
    }

    /***
     * ?????? ??????????????????
     */
    public static String getStr(String key ){
        StatefulRedisConnection<String, String> redisConnection = null;
        try {
            redisConnection = pool.borrowObject();
            RedisCommands<String, String> sync = redisConnection.sync();
            String data = sync.get(key);
            return data;

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if (null != redisConnection) {
                pool.returnObject(redisConnection);
            }
        }
        return null;
    }
    /***
     * ????????????????????????
     */
    public static Boolean existsLastAlarmStatus(String ruleId,String uniqueDataPointId ){
        StatefulRedisConnection<String, String> redisConnection = null;
        try {
            redisConnection = pool.borrowObject();
            RedisCommands<String, String> sync = redisConnection.sync();

            return sync.hexists(RuleCacheConstant.LAST_DATAPOINTALARMDATA_PREFIX+ruleId, uniqueDataPointId);

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if (null != redisConnection) {
                pool.returnObject(redisConnection);
            }
        }
        return null;
    }

    /***
     * ???????????????????????????
     */
    public static Boolean setLastAlarmStatus(String ruleId,String uniqueDataPointId,String data ){
        StatefulRedisConnection<String, String> redisConnection = null;
        try {
            redisConnection = pool.borrowObject();
            RedisCommands<String, String> sync = redisConnection.sync();

            return sync.hset(RuleCacheConstant.LAST_DATAPOINTALARMDATA_PREFIX+ruleId, uniqueDataPointId,data);

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if (null != redisConnection) {
                pool.returnObject(redisConnection);
            }
        }
        return null;
    }

    /***
     *  ???????????????????????????
     */
    public static void delLastAlarmStatus(String ruleId,String uniqueDataPointId){
        StatefulRedisConnection<String, String> redisConnection = null;
        try {
            redisConnection = pool.borrowObject();
            RedisCommands<String, String> sync = redisConnection.sync();

            sync.hdel(RuleCacheConstant.LAST_DATAPOINTALARMDATA_PREFIX+ruleId, uniqueDataPointId);

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if (null != redisConnection) {
                pool.returnObject(redisConnection);
            }
        }
        return ;
    }
    /***
     * ?????????????????????????????????
     */
    public static Boolean existsDatapointAlaramPush(String ruleId,String uniqueDataPointId ){
        StatefulRedisConnection<String, String> redisConnection = null;
        try {
            redisConnection = pool.borrowObject();
            RedisCommands<String, String> sync = redisConnection.sync();

            return sync.hexists(RuleCacheConstant.DATAPOINTALARMPUSH_PREFIX+ruleId,uniqueDataPointId );

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if (null != redisConnection) {
                pool.returnObject(redisConnection);
            }
        }
        return null;
    }
    /***
     * ?????????????????????????????????
     */
    public static void setDatapointAlaramPushStatus(String ruleId,String uniqueDataPointId  ){
        StatefulRedisConnection<String, String> redisConnection = null;
        try {
            redisConnection = pool.borrowObject();
            RedisCommands<String, String> sync = redisConnection.sync();

            sync.hset(RuleCacheConstant.DATAPOINTALARMPUSH_PREFIX+ruleId,uniqueDataPointId,"1" );

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if (null != redisConnection) {
                pool.returnObject(redisConnection);
            }
        }
        return;
    }
    /***
     * ?????????????????????????????????
     */
    public static void delDatapointAlaramPushStatus(String ruleId,String uniqueDataPointId ){
        StatefulRedisConnection<String, String> redisConnection = null;
        try {
            redisConnection = pool.borrowObject();
            RedisCommands<String, String> sync = redisConnection.sync();

            sync.hdel(RuleCacheConstant.DATAPOINTALARMPUSH_PREFIX+ruleId,uniqueDataPointId);

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if (null != redisConnection) {
                pool.returnObject(redisConnection);
            }
        }
        return;
    }



    public static void main(String[] args) {

       Map<String,Object> map = new HashMap<>(16);
       map.put("id",1);
        map.put("alarmRuleName","????????????");
        map.put("monitorObj","0");
        map.put("triggerCondition","2");
        map.put("triggerConditionAVal","20");
        map.put("triggerConditionBVal","20");
        map.put("triggerConditionValType","0");
        map.put("alarmMsg","????????????");
        map.put("alarmDeadZone","0");
        map.put("delayTime","10");
        map.put("pushStatus","1");
        map.put("pushFrequency","1");
        map.put("pushInterval",10);
        // device_id sn unique_data_point_id template_id data_point_id
        map.put("monitorElement","10-sn123-1-null-null#10-sn123-2-null-null");


        Map<String,Object> map1 = new HashMap<>(16);
        map1.put("id",2);
        map1.put("alarmRuleName","????????????2");
        map1.put("monitorObj","0");
        map1.put("triggerCondition","2");
        map1.put("triggerConditionAVal","20");
        map1.put("triggerConditionBVal","20");
        map1.put("triggerConditionValType","0");
        map1.put("alarmMsg","????????????2");
        map1.put("alarmDeadZone","0");
        map1.put("delayTime","10");
        map1.put("pushStatus","1");
        map1.put("pushFrequency","1");
        map1.put("push_interval",10);
        // device_id sn unique_data_point_id template_id data_point_id
        map1.put("monitorElement","10-sn123-1-null-null#10-sn123-2-null-null");


        Map<String,Object> adMap = new HashMap<>(16);

        RedisUtil.setStr("a_"+1, JSONObject.toJSONString(map));
        RedisUtil.setStr("a_"+2,JSONObject.toJSONString(map1));

        RedisUtil.setHash("ad_"+"sn123","1","1,2");
        RedisUtil.setHash("ad_"+"sn123","2","1,2");

    }

    public static void setUniqueDataPointRuleMapper(String sn ,String uniqueDataPointId, String ruleIds) {
        String ruleUniquedatapointCachePrefix = RuleCacheConstant.RULE_UNIQUEDATAPOINT_CACHE_PREFIX;
        RedisUtil.setHash(ruleUniquedatapointCachePrefix+sn,ruleUniquedatapointCachePrefix+uniqueDataPointId,ruleIds);
    }

    public static void setUniqueDataPointRuleMapper(String uniqueDataPointId, String ruleIds) {
        String ruleUniquedatapointCachePrefix = RuleCacheConstant.RULE_UNIQUEDATAPOINT_CACHE_PREFIX;
        RedisUtil.setStr(ruleUniquedatapointCachePrefix+uniqueDataPointId,ruleIds);
    }

    public static String getUniqueDataPointRuleMapper(String uniqueDataPointId) {
        String ruleUniquedatapointCachePrefix = RuleCacheConstant.RULE_UNIQUEDATAPOINT_CACHE_PREFIX;
        return RedisUtil.getStr(ruleUniquedatapointCachePrefix+uniqueDataPointId);
    }

    public static String getDataPointRuleMapper(String dataPointId) {
        String ruleUniquedatapointCachePrefix = RuleCacheConstant.RULE_DATAPOINT_CACHE_PREFIX;
        return RedisUtil.getStr(ruleUniquedatapointCachePrefix+dataPointId);
    }


    public static void aa(){
        StatefulRedisConnection<String, String> redisConnection = null;
        try {
            redisConnection = pool.borrowObject();
            RedisCommands<String, String> sync = redisConnection.sync();


        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if (null != redisConnection) {
                pool.returnObject(redisConnection);
            }
        }
    }

    public static void setDevicePointRel(Map<String, List<DevicePointRelDto>> map) {
        Set<String> sns = map.keySet();
        StatefulRedisConnection<String, String> redisConnection = null;
        try {
            redisConnection = pool.borrowObject();
            RedisCommands<String, String> sync = redisConnection.sync();
            for (String sn : sns) {
                List<DevicePointRelDto> devicePointRelDtos = map.get(sn);
                Map<String, String> collect = devicePointRelDtos.stream().collect(Collectors.toMap(key -> {
                    return key.getDeviceTemplatePointId().toString();
                }, val -> {
                    return val.getId().toString();
                }));
                sync.hset(RuleCacheConstant.DEVICE_DATAPOINT_PREFIX+sn,collect);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if (null != redisConnection) {
                pool.returnObject(redisConnection);
            }
        }
    }

    public static void delDevicePointRel(List<String> snList) {
        StatefulRedisConnection<String, String> redisConnection = null;
        try {
            redisConnection = pool.borrowObject();
            RedisCommands<String, String> sync = redisConnection.sync();
            String[] tmp = new String[snList.size()];

            for (int i = 0; i < snList.size(); i++) {
                tmp[i] = RuleCacheConstant.DEVICE_DATAPOINT_PREFIX+snList.get(i);
            }
            sync.del(tmp);

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if (null != redisConnection) {
                pool.returnObject(redisConnection);
            }
        }
    }

    public static String getDevicePointRel(String sn, String dataPointId) {
        StatefulRedisConnection<String, String> redisConnection = null;
        try {
            redisConnection = pool.borrowObject();
            RedisCommands<String, String> sync = redisConnection.sync();
            String uniqueDataPointId = sync.hget(RuleCacheConstant.DEVICE_DATAPOINT_PREFIX + sn, dataPointId);
            if (CommonUtil.judgeEmpty(uniqueDataPointId)) {
                List<DevicePointRelDto> listBySn = MybatisConfig.getMapper(DeviceTemplatePointRelMapper.class).getListBySn(sn);
                Map<String, String> collect = listBySn.stream().collect(Collectors.toMap(key -> {
                    return key.getDeviceTemplatePointId().toString();
                }, val -> {
                    return val.getId().toString();
                }));
                sync.hset(RuleCacheConstant.DEVICE_DATAPOINT_PREFIX+sn,collect);
                for (DevicePointRelDto devicePointRelDto : listBySn) {
                    if (devicePointRelDto.getDeviceTemplatePointId().toString().equals(dataPointId)){
                        uniqueDataPointId = devicePointRelDto.getId().toString();
                        break;
                    }
                }
            }
            return uniqueDataPointId;

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if (null != redisConnection) {
                pool.returnObject(redisConnection);
            }
        }
        return null;
    }
}
