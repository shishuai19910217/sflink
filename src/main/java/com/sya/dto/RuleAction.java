package com.sya.dto;

import lombok.Data;

import java.io.Serializable;
@Data
public class RuleAction implements Serializable {
    private String executeMode;
    private String actionType;
    private String action;
    private Integer ruleId;
}
