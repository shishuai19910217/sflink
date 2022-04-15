package com.sya.mapper;
import com.sya.kafka.datapointalarm.rule.dto.MachineDeviceRelDto;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * <p>
 * 设备-网关关联表 Mapper 接口
 * </p>
 *
 * @author liujinqi
 * @since 2021-11-15
 */
public interface MachineDeviceRelMapper {
    public List<MachineDeviceRelDto> getList(@Param("sns") List<String> sns  );


}
