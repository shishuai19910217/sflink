package com.sya.kafka.datapointalarm.rule.conversion;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.sya.kafka.datapointalarm.rule.dto.DataPointDto;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/***
 * 数据点上报的原始数据进行压平处理
 */
public class KafkaDataFlatMapper implements FlatMapFunction<JSONObject, DataPointDto> {
    @Override
    public void flatMap(JSONObject jsonObject, Collector<DataPointDto> collector) throws Exception {
        JSONArray dataPoints = jsonObject.getJSONArray("dataPoints");

        if (null != dataPoints && dataPoints.size()>0) {
            String sn = jsonObject.getString("deviceId");
            int size = dataPoints.size();
            JSONObject datapointForkafka = null;
            DataPointDto dto = null;
            Integer err = null;
            for (int i = 0; i < size; i++) {
                datapointForkafka = dataPoints.getJSONObject(i);
                dto = new DataPointDto();
                err = datapointForkafka.getInteger("err");
                if (null == err ||err.intValue()!=0) {
                    continue;
                }
                if (datapointForkafka.containsKey("dataPointId")) {
                    dto.setUniqueDataPointId(datapointForkafka.getInteger("dataPointId"));
                }
                if (datapointForkafka.containsKey("timeMs")) {
                    dto.setTimeMs(datapointForkafka.getLong("timeMs"));
                }
                if (datapointForkafka.containsKey("value")) {
                    dto.setVal(datapointForkafka.getString("value"));
                }
                dto.setSn(sn);
                collector.collect(dto);

            }
        }
    }
}
