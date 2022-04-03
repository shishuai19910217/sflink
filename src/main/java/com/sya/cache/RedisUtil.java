package com.sya.cache;

import com.alibaba.fastjson.JSONObject;
import com.sya.kafka.datapointalarm.AlarmRuleDto;
import com.sya.kafka.datapointalarm.AlarmRuleMonitorElementDto;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class RedisUtil {
    private static final GenericObjectPool<StatefulRedisConnection<String, String>> pool;

    static {

        RedisURI redisUri = RedisURI.Builder.
                redis("139.196.142.241",6379)
                .withDatabase(2).build();
        redisUri.setPassword("nuojiehongkao2022".toCharArray());
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
     * 获取 监控对象为设备的报警缓存
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
     * key 为uniqueDataPointId  val为 代表变量报警规则的主键id
     * 123:"1,2,3", // 123 代表unique_data_point_id ；1,2,3 代表变量报警规则的主键id
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
     * 获取 变量报警规则
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
     * 获取 变量报警规则
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
     * 是否存在上次报警
     */
    public static Boolean existsLastAlarmStatus(String uniqueDataPointId,String ruleId ){
        StatefulRedisConnection<String, String> redisConnection = null;
        try {
            redisConnection = pool.borrowObject();
            RedisCommands<String, String> sync = redisConnection.sync();

            return sync.hexists("lastdatapointalarmstatus_"+uniqueDataPointId, ruleId);

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
     * 保存最新的报警状态
     */
    public static Boolean setLastAlarmStatus(String uniqueDataPointId,String ruleId,String data ){
        StatefulRedisConnection<String, String> redisConnection = null;
        try {
            redisConnection = pool.borrowObject();
            RedisCommands<String, String> sync = redisConnection.sync();

            return sync.hset("lastdatapointalarmstatus_"+uniqueDataPointId, ruleId,data);

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
     *  删除最新的报警状态
     */
    public static void delLastAlarmStatus(String uniqueDataPointId,String ruleId ){
        StatefulRedisConnection<String, String> redisConnection = null;
        try {
            redisConnection = pool.borrowObject();
            RedisCommands<String, String> sync = redisConnection.sync();

            sync.hdel("lastdatapointalarmstatus_"+uniqueDataPointId, ruleId);

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
     * 某个规则是不是推送过了
     */
    public static Boolean existsDatapointAlaramPush(String ruleId,String uniqueDataPointId ){
        StatefulRedisConnection<String, String> redisConnection = null;
        try {
            redisConnection = pool.borrowObject();
            RedisCommands<String, String> sync = redisConnection.sync();

            return sync.hexists("datapointalarmpushsttus_"+ruleId,uniqueDataPointId );

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
     * 某个规则是不是推送过了
     */
    public static void setDatapointAlaramPushStatus(String ruleId,String uniqueDataPointId ){
        StatefulRedisConnection<String, String> redisConnection = null;
        try {
            redisConnection = pool.borrowObject();
            RedisCommands<String, String> sync = redisConnection.sync();

            sync.hset("datapointalarmpushsttus_"+ruleId,uniqueDataPointId,"1" );

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
     * 某个规则是不是推送过了
     */
    public static void delDatapointAlaramPushStatus(String ruleId,String uniqueDataPointId ){
        StatefulRedisConnection<String, String> redisConnection = null;
        try {
            redisConnection = pool.borrowObject();
            RedisCommands<String, String> sync = redisConnection.sync();

            sync.hdel("datapointalarmpushsttus_"+ruleId,uniqueDataPointId);

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

//       Map<String,Object> map = new HashMap<>(16);
//       map.put("id",1);
//        map.put("alarmRuleName","报警规则");
//        map.put("monitorObj","0");
//        map.put("triggerCondition","2");
//        map.put("triggerConditionAVal","20");
//        map.put("triggerConditionBVal","20");
//        map.put("triggerConditionValType","0");
//        map.put("alarmMsg","我报警了");
//        map.put("alarmDeadZone","0");
//        map.put("delayTime","10");
//        map.put("pushStatus","1");
//        map.put("pushFrequency","1");
//        map.put("pushInterval",10);
//        // device_id sn unique_data_point_id template_id data_point_id
//        map.put("monitorElement","10-sn123-1-null-null#10-sn123-2-null-null");
//
//
//        Map<String,Object> map1 = new HashMap<>(16);
//        map1.put("id",2);
//        map1.put("alarmRuleName","报警规则2");
//        map1.put("monitorObj","0");
//        map1.put("triggerCondition","2");
//        map1.put("triggerConditionAVal","20");
//        map1.put("triggerConditionBVal","20");
//        map1.put("triggerConditionValType","0");
//        map1.put("alarmMsg","我报警了2");
//        map1.put("alarmDeadZone","0");
//        map1.put("delayTime","10");
//        map1.put("pushStatus","1");
//        map1.put("pushFrequency","1");
//        map1.put("push_interval",10);
//        // device_id sn unique_data_point_id template_id data_point_id
//        map1.put("monitorElement","10-sn123-1-null-null#10-sn123-2-null-null");
//
//
//        Map<String,Object> adMap = new HashMap<>(16);
//
//        RedisUtil.setStr("a_"+1, JSONObject.toJSONString(map));
//        RedisUtil.setStr("a_"+2,JSONObject.toJSONString(map1));
//
//        RedisUtil.setHash("ad_"+"sn123","1","1,2");
//        RedisUtil.setHash("ad_"+"sn123","2","1,2");
        System.out.printf( RedisUtil.getAlarmRuleData("1").toString());

    }
}
