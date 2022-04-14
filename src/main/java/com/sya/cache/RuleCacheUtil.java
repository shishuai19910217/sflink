package com.sya.cache;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.sya.config.MybatisConfig;
import com.sya.constants.RuleCacheConstant;
import com.sya.dto.*;
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
public class RuleCacheUtil {

    public static List<RuleMonitorElement> getRuleMonitorElementForDB(List<Integer> uniqueDataPointIds){
        RuleMonitorElementMapper mapper = MybatisConfig.getMapper(RuleMonitorElementMapper.class);
        List<RuleMonitorElement> list = mapper.getList(uniqueDataPointIds);
        return list;
    }
    public static List<RuleMonitorElement> getRuleMonitorElementForDB(Integer uniqueDataPointId){
        RuleMonitorElementMapper mapper = MybatisConfig.getMapper(RuleMonitorElementMapper.class);
        List<Integer> ids = new ArrayList<>();
        ids.add(uniqueDataPointId);
        List<RuleMonitorElement> list = mapper.getList(ids);
        return list;
    }

    public static List<RuleBaseCacheDto> getBaseRuleForDB(List<Integer> ruleIds) {
        return null;
    }

    /***
     * 更新 关系数据点与规则的映射关系
     * @param key
     * @param ruleIdStr
     */
    public static void setUniqueDataPointRuleMapper(String key, String ruleIdStr) {
        CacheUtil.set(key,ruleIdStr);
    }

    /***
     * 获取 规则的详细信息 包含元素和动作
     * @param ruleIds
     * @return
     */
    public static List<RuleBaseDto> getRuleBaseDataForDB(List<Integer> ruleIds) {
        RuleMapper mapper = MybatisConfig.getMapper(RuleMapper.class);
        List<RuleDto> list = mapper.getByRuleIds(ruleIds);
        List<RuleAction> actions = mapper.getRuleActionByRuleIds(ruleIds);
        if (CommonUtil.judgeEmpty(list,actions)) {
            return null;
        }
        Map<String, List<RuleDto>> rules = list.stream().collect(Collectors.groupingBy(action -> {
            return action.getId().toString();
        }));

        Map<String, RuleAction> actionMap = actions.stream().
                collect(
                        Collectors.toMap(action -> {
                                    return action.getRuleId().toString();
                                },
                                val -> {
                                    return val;
                                }));
        Set<String> ruleIdsForDB = rules.keySet();
        RuleBaseDto ruleBaseDto = null;
        List<RuleDto> ruleDtos = null;
        RuleMonitorElement element = null;
        List<RuleBaseDto> returnList = new ArrayList<>();
        for (String id : ruleIdsForDB) {
            ruleDtos = rules.get(id);
            RuleDto ruleDto = ruleDtos.get(0);
            ruleBaseDto = new RuleBaseDto(ruleDto.getId(),ruleDto.getTrigger(),ruleDto.getRelyRuleId()
            ,ruleDto.getTriggerType());
            returnList.add(ruleBaseDto);
            for (RuleDto dto : ruleDtos) {
                element = new RuleMonitorElement(dto.getId(),dto.getElementType(),dto.getMachineId(),
                        dto.getUniqueDataPointId(),dto.getTemplateId(),dto.getDataPointId(),dto.getSn(),
                        dto.getOperaDataPointId());
                ruleBaseDto.getElementList().add(element);
            }
            ruleBaseDto.setRuleAction(actionMap.get(id));

        }
        return returnList;

    }

    /***
     * 保存规则详细信息
     * @param ruleBaseDataForDB
     */
    public static void setRuleBaseData(List<RuleBaseDto> ruleBaseDataForDB) {
        if (CommonUtil.judgeEmpty(ruleBaseDataForDB)) {
            return;
        }
        Map<String, Object> collect = ruleBaseDataForDB.stream().collect(Collectors.toMap(key -> {
            return key.getId().toString();
        }, val -> {
            return val;
        }));
        CacheUtil.set(collect);
    }


    public static RuleBaseDto getRuleBaseData(String ruleId){
        String str = CacheUtil.getStr(ruleId);
        return JSONObject.parseObject(str,RuleBaseDto.class);
    }

    /***
     * 获取 报警规则关联的控制规则
     * @param relyRuleId
     * @return
     */
    public static List<Integer> getRuleBaseDataByRelyRuleId(String relyRuleId) {
        String data = CacheUtil.getStr(RuleCacheConstant.ALARM_RELY_RULEIDS_PREFIX+relyRuleId);
        if (CommonUtil.judgeEmpty(data)) {
            return null;
        }
        String[] split = data.split(",");
        List<Integer> ruleIds = new ArrayList<>();
        for (String s : split) {
            ruleIds.add(Integer.parseInt(s));
        }
        return ruleIds;
    }
}
