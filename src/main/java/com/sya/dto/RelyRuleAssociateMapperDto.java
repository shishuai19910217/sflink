package com.sya.dto;

import lombok.Data;

import java.io.Serializable;

@Data
public class RelyRuleAssociateMapperDto implements Serializable {
    private Integer controlId;
    /**
     * 依赖规则
     */
    private Integer alarmId;
}
