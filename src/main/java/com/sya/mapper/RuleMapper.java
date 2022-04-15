package com.sya.mapper;

import com.sya.dto.RelyRuleAssociateMapperDto;
import com.sya.dto.RuleAction;
import com.sya.dto.RuleDto;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface RuleMapper {
    /***
     * 获取规则与元素
     * @param ruleIds
     * @return
     */
    List<RuleDto> getByRuleIds(@Param("ids") List<Integer> ruleIds);

    /***
     * 获取 动作
     * @param ruleIds
     * @return
     */
    List<RuleAction> getRuleActionByRuleIds(@Param("ids")  List<Integer> ruleIds);

    List<RelyRuleAssociateMapperDto> getByRelyRuleIds(@Param("ids") List<Integer> relyRuleIds);
}
