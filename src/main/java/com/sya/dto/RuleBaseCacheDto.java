package com.sya.dto;

import lombok.Data;

import java.io.Serializable;

@Data
public class RuleBaseCacheDto implements Serializable {
    private String trigger;
    private Integer relyRuleId;
    private String triggerType;
    private Integer id;
    //{element_type}-{device_id}-{unique_data_point_id}-{template_id}-{data_point_id}-{sn}-{opera_data_point_id}
    private String monitorElement;

}
