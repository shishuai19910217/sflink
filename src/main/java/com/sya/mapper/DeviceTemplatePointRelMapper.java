package com.sya.mapper;

import com.sya.kafka.datapointalarm.rule.dto.DevicePointRelDto;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface DeviceTemplatePointRelMapper {
    List<DevicePointRelDto> getList(@Param("sns") List<String> sns);

    List<DevicePointRelDto> getListBySn(@Param("sn") String sns);
}
