package com.sya.mapper;
import com.sya.dto.MachineDeviceRel;
import com.sya.dto.RuleMonitorElement;
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
public interface RuleMonitorElementMapper {
    List<RuleMonitorElement> getList(@Param("uniqueDataPointIds") List<Integer> uniqueDataPointIds);
}
