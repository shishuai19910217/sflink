package com.sya.mapper;
import com.sya.dto.MachineDeviceRel;
import org.apache.ibatis.annotations.Param;

/**
 * <p>
 * 设备-网关关联表 Mapper 接口
 * </p>
 *
 * @author liujinqi
 * @since 2021-11-15
 */
public interface MachineDeviceRelMapper {
    public  MachineDeviceRel getList(@Param("id") Integer id  );
}
