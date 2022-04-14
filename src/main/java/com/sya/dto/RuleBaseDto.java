package com.sya.dto;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
public class RuleBaseDto implements Serializable {

    private Integer id;
    private String trigger;
    private String relyRuleId;
    private String triggerType;
    private List<RuleMonitorElement> elementList = new ArrayList<>();
    private RuleAction ruleAction;
    public RuleBaseDto(){}
    public RuleBaseDto(Integer id, String trigger, String relyRuleId, String triggerType) {
        this.id = id;
        this.trigger = trigger;
        this.relyRuleId = relyRuleId;
        this.triggerType = triggerType;
    }
}
