package com.sya.kafka.datapointalarm.rule.dto;

import lombok.Data;

import java.io.Serializable;

@Data
public class DevicePointRelDto implements Serializable {
    private Integer id;
    private String sn;
    private Integer deviceTemplatePointId;
}
