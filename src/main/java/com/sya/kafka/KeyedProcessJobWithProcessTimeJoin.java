package com.sya.kafka;

import com.sya.kafka.source.SourceBuilder;
import com.sya.dto.ClockDto;
import com.sya.dto.SNStatusDto;
import com.sya.dto.SnAlaram;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/***
 * 离线超过5分钟报警
 */
public class KeyedProcessJobWithProcessTimeJoin {
    public static void main(String[] args) throws Exception {
        // 获取 flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("localhost", 9999);
        //env.setParallelism(1);
        // 设置状态后端
        OutputTag<SnAlaram> alramTag = new OutputTag<SnAlaram>("sn-alarm"){};
        DataStreamSource<ClockDto> clockDtoDataStreamSource = env.addSource(new SourceBuilder().getMySensorSource());
        DataStream<SNStatusDto> mapStream = stringDataStreamSource.map(new SNStatusMapper());
        SingleOutputStreamOperator<SNStatusDto> connectSteam = mapStream.connect(clockDtoDataStreamSource).process(new CoProcessFunction<SNStatusDto, ClockDto, SNStatusDto>() {
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<SNStatusDto> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
            }

            @Override
            public void processElement1(SNStatusDto value, Context ctx, Collector<SNStatusDto> out) throws Exception {
                out.collect(value);
            }

            @Override
            public void processElement2(ClockDto value, Context ctx, Collector<SNStatusDto> out) throws Exception {
                out.collect(new SNStatusDto(value.getTimestamp(), value.getStatusVal(), value.getSn()));
            }
        });
        KeyedStream<SNStatusDto, String> keyedStream = connectSteam.keyBy(new KeySelector<SNStatusDto, String>() {
            @Override
            public String getKey(SNStatusDto o) throws Exception {
                return o.getSn();
            }
        });
        // 拆流
        SingleOutputStreamOperator<SNStatusDto> process = keyedStream
                .process(new SingleOutKeyedProcessFunction(alramTag));
        // 处理真正的报警流
        process.getSideOutput(alramTag).keyBy(new KeySelector<SnAlaram, String>() {
            @Override
            public String getKey(SnAlaram snAlaram) throws Exception {
                return snAlaram.getSn();
            }
        }).process(new MyAlarmKeyedProcessFunction()).print();

        env.execute();
    }

    /***
     * 报警频率 网关离线超过 N分钟
     *
     */
    static class MyAlarmKeyedProcessFunction extends KeyedProcessFunction<String, SnAlaram, SnAlaram> {
        public MyAlarmKeyedProcessFunction() {
            super();
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<SnAlaram> out) throws Exception {
            // 其实就是数据入库
            String currentKey = ctx.getCurrentKey();
            System.out.println(currentKey+"---报警了---"+ctx.timerService().currentProcessingTime());
            long timeInterval = 0L;
            if (currentKey.equals("a")) {
                timeInterval = 1000L;
            }else {
                timeInterval = 5000L;
            }
            ValueState<Long> sn_timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>(currentKey+"_alramTimerTs", Long.class));
            // 再设置 下次报警时间
            Long value = sn_timerTs.value();
            ctx.timerService().registerProcessingTimeTimer(value+timeInterval);
            sn_timerTs.update(value+timeInterval);

        }

        @Override
        public void processElement(SnAlaram value, Context ctx, Collector<SnAlaram> out) throws Exception {
            String sn = value.getSn();
            String status = value.getStatus();
            long timeInterval = 0L;

            if (sn.equals("a")) {
                timeInterval = 1000L;
            }else {
                timeInterval = 5000L;
            }
            /***
             * 起一个
             */
            // 定时任务
            ValueState<Long> sn_timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>(sn+"_alramTimerTs", Long.class));
            Long sn_timerTsVal = sn_timerTs.value();
            // 报警中
            if ("0".equals(status)) {
                if (null == sn_timerTsVal) {
                    // 计算出 定时任务的时间戳
                    long ts = ctx.timerService().currentProcessingTime();
                    ts = ts + timeInterval; // 五秒
                    ctx.timerService().registerProcessingTimeTimer(ts);
                    System.out.println("----所属的时间窗是---"+ts);
                    sn_timerTs.update(ts);
                }else {
                    System.out.println("----所属的时间窗是---"+sn_timerTsVal);
                }
            }else {
                // 删除 相关定时任务
                // 计算出 定时任务的时间戳
                if (null != sn_timerTsVal) {
                    ctx.timerService().deleteProcessingTimeTimer(sn_timerTsVal);
                    sn_timerTs.clear();
                }

            }
        }
    }

    /***
     * 将原始流 拆出 N分钟不在线的处理流
     */
    static class SingleOutKeyedProcessFunction  extends KeyedProcessFunction<String, SNStatusDto, SNStatusDto>  implements CheckpointedFunction{

        private  OutputTag<SnAlaram> alramTag;


        public SingleOutKeyedProcessFunction(OutputTag<SnAlaram> alramTag) {
            this.alramTag = alramTag;
        }
        @Override
        public void processElement(SNStatusDto value, Context ctx, Collector<SNStatusDto> out) throws Exception {
            String val = value.getStatusVal();
            String sn = value.getSn();
            // 定时任务
            ValueState<Long> sn_timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>(sn+"_TimerTs", Long.class));
            Long sn_timerTsVal = sn_timerTs.value();
            Long timeInterval = 0L;
            if (sn.equals("a")) {
                timeInterval = 5000L;
            }else {
                timeInterval = 10000L;
            }

            // 本次不在线
            if ("0".equals(val)) {
                if (null == sn_timerTsVal) {
                    // 计算出 定时任务的时间戳
                    long ts = ctx.timerService().currentProcessingTime();
                    ts = ts + timeInterval;
                    ctx.timerService().registerProcessingTimeTimer(ts);
                    sn_timerTs.update(ts);
                }
            }else {
                // 删除 相关定时任务
                // 计算出 定时任务的时间戳
                if (null != sn_timerTsVal) {
                    ctx.timerService().deleteProcessingTimeTimer(sn_timerTsVal);
                    sn_timerTs.clear();
                }
                // 恢复流
                ctx.output(alramTag,new SnAlaram(sn,1,"报警了解除",ctx.timerService().currentProcessingTime(),"1"));
                // 删除 报警记录吧
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<SNStatusDto> out) throws Exception {
            // 其实就是数据入库
            String currentKey = ctx.getCurrentKey();
            long ts = ctx.timerService().currentProcessingTime();
            ValueState<Long> sn_timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>(currentKey+"_TimerTs", Long.class));
            // 报警流  侧出流
            ctx.output(alramTag,new SnAlaram(currentKey,1,"报警了",ts,"0"));
            sn_timerTs.clear();
        }
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            //
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {

        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {

        }
    }



    /***
     * 将数据转成 SNStatusDto
     */
    static class SNStatusMapper implements MapFunction<String, SNStatusDto> {
        @Override
        public SNStatusDto map(String s) throws Exception {
            String[] split = s.split(",");
            String sn = split[0];
            String statusVal = split[1];
            String time = split[2];
            return new SNStatusDto(Long.parseLong(time),statusVal,sn);
        }
    }
}
