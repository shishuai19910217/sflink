package com.sya.kafka.datapointalarm.rule.dto;

import lombok.Data;

import java.io.Serializable;
@Data
public class MachineDeviceRelDto implements Serializable {
    private String deviceSn;
    private Integer machineId;
    private Integer id;
}
