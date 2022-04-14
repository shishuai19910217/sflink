package com.sya.kafka;

import com.sya.dto.WInData;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyStateJob {
    public static void main(String[] args) throws Exception {
        // 获取 flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("localhost", 9999);
        env.setParallelism(1);
        // 设置状态后端
        env.setStateBackend(new RocksDBStateBackend(""));
        DataStream<WInData> mapStream = stringDataStreamSource.map(new Mymapper());



        env.execute();
    }
    static class Mymapper extends RichMapFunction<String, WInData> {


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 声明 keyed 状态
            MapState<Object, Object> mapState = getRuntimeContext().getMapState(null);
            /***
             * 是否可以
             */

        }


        @Override
        public WInData map(String s) throws Exception {
            return null;
        }
    }
}
