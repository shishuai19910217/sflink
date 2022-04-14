package com.sya.dto;

import lombok.Data;

/**
 * <p>
 * 设备-网关关联表
 * </p>
 *
 * @author liujinqi
 * @since 2021-11-15
 */
@Data
public class MachineDeviceRel {

    private static final long serialVersionUID=1L;

    private Integer id;

    /**
     * 设备ID
     */
    private Integer machineId;

    /**
     * 网关SN
     */
    private String deviceSn ;


}
