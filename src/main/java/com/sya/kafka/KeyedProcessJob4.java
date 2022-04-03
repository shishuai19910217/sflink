package com.sya.kafka;

import dto.SNDisconnectedDto;
import dto.SNStatusDto;
import dto.SnAlaram;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class KeyedProcessJob4 {
    public static void main(String[] args) throws Exception {
        // 获取 flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);
        DataStream<SNStatusDto> snStatusDtoDataStream = dataStreamSource.map(new SNStatusMapper());
        OutputTag<SNStatusDto> disconnectedOutputTag = new OutputTag<SNStatusDto>("disconnected"){};
        // 原始流
        SingleOutputStreamOperator<SNStatusDto> process = snStatusDtoDataStream.process(new SideOutputProcessFunction(disconnectedOutputTag));
        //        // 提取事件时间
        WatermarkStrategy<SNStatusDto> wm = new WatermarkStrategy<SNStatusDto>() {
            @Override
            public WatermarkGenerator<SNStatusDto> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new MyBoundedOutOfOrdernessGenerator(0);
            }
        }.withTimestampAssigner(new SerializableTimestampAssigner<SNStatusDto>() {
            @Override
            public long extractTimestamp(SNStatusDto element, long recordTimestamp) {
                return element.getTimestamp();
            }
        });

        // 专门处理 网关N分钟内，掉线次数超过 N次
        DataStream<SNStatusDto> disconnectedDataStream = process.getSideOutput(disconnectedOutputTag);
        KeyedStream<SNStatusDto, String> snStatusDtoStringKeyedStream = disconnectedDataStream
//                .assignTimestampsAndWatermarks(WatermarkStrategy
//                        .<SNStatusDto>forBoundedOutOfOrderness(Duration.ofSeconds(0))
//                        .withTimestampAssigner(new SerializableTimestampAssigner<SNStatusDto>() {
//                                                   @Override
//                                                   public long extractTimestamp(SNStatusDto snStatusDto, long l) {
//                                                       return snStatusDto.getTimestamp();
//                                                   }
//                                               }
//
//                        ))
                .assignTimestampsAndWatermarks(wm.withIdleness(Duration.ofSeconds(1)))
                .keyBy(new KeySelector<SNStatusDto, String>() {
            @Override
            public String getKey(SNStatusDto snStatusDto) throws Exception {
                return snStatusDto.getSn();
            }
        });

        Pattern<SNStatusDto, SNStatusDto> disconnectedPattern = getDisconnectedPattern("online", "off");
        PatternStream<SNStatusDto> pattern = CEP.pattern(snStatusDtoStringKeyedStream, disconnectedPattern);

        SingleOutputStreamOperator<SNDisconnectedDto> select =
                pattern.select(new DisconnectedPatternSelectFunction("online", "off"));
        KeyedStream<SNDisconnectedDto, String> snDisconnectedDtoStringKeyedStream =
                select.keyBy(new KeySelector<SNDisconnectedDto, String>() {
            @Override
            public String getKey(SNDisconnectedDto snDisconnectedDto) throws Exception {
                return snDisconnectedDto.getSn();
            }
        });
        OutputTag<SNStatusDto> disconnectedAlarmOutputTag = new OutputTag<SNStatusDto>("disconnectedAlarmOutputTag"){};

        SingleOutputStreamOperator<SnAlaram> process1 = snDisconnectedDtoStringKeyedStream.process(new SnAlaramKeyedProcessFunction(disconnectedAlarmOutputTag));
        process1.print();


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
            System.out.println(currentKey+"---报警了---"+ctx.timestamp());
            long timeInterval = 0L;
            if (currentKey.equals("a")) {
                timeInterval = 1000L;
            }else {
                timeInterval = 5000L;
            }
            ValueState<Long> sn_timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>(currentKey+"_alramTimerTs", Long.class));
            // 再设置 下次报警时间
            Long value = sn_timerTs.value();
            ctx.timerService().registerEventTimeTimer(value+timeInterval);
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
                    long ts = ctx.timestamp();
                    ts = ts + timeInterval; // 五秒
                    ctx.timerService().registerEventTimeTimer(ts);
                    System.out.println("----所属的时间窗是---"+ts);
                    sn_timerTs.update(ts);
                }else {
                    System.out.println("----所属的时间窗是---"+sn_timerTsVal);
                }
            }else {
                // 删除 相关定时任务
                // 计算出 定时任务的时间戳
                if (null != sn_timerTsVal) {
                    ctx.timerService().deleteEventTimeTimer(sn_timerTsVal);
                    sn_timerTs.clear();
                }

            }
        }
    }

    static  class MyBoundedOutOfOrdernessGenerator implements WatermarkGenerator<SNStatusDto> {

        private  long maxOutOfOrderness = 1000; // 1 秒

        private long currentMaxTimestamp;

        public MyBoundedOutOfOrdernessGenerator(long maxOutOfOrderness) {
            this.maxOutOfOrderness = maxOutOfOrderness;
        }

        @Override
        public void onEvent(SNStatusDto event, long eventTimestamp, WatermarkOutput output) {
            currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 发出的 watermark = 当前最大时间戳 - 最大乱序时间

            long timestamp = currentMaxTimestamp - maxOutOfOrderness - 1;
            System.out.println("----可能发出的 watermark "+ timestamp + "--currentMaxTimestamp "+currentMaxTimestamp);
            output.emitWatermark(new Watermark(timestamp));
        }

    }
    /***
     * 判断在规定时间内掉线的次数
     *
     */
    static class SnAlaramKeyedProcessFunction extends KeyedProcessFunction<String, SNDisconnectedDto, SnAlaram> {
        OutputTag<SNStatusDto> disconnectedAlarmOutputTag;

        public SnAlaramKeyedProcessFunction(OutputTag<SNStatusDto> disconnectedAlarmOutputTag) {
            this.disconnectedAlarmOutputTag = disconnectedAlarmOutputTag;
        }

        public SnAlaramKeyedProcessFunction() {
        }

        @Override
        public void processElement(SNDisconnectedDto value, Context ctx, Collector<SnAlaram> out) throws Exception {
            String sn = value.getSn();
            // 获取当前数据应该属于的定时窗口
            RuntimeContext runtimeContext = getRuntimeContext();
            Long onlineEventTimestamp = value.getOnlineEventTimestamp();
            ValueState<Long> snTimerValueState = runtimeContext.getState(new ValueStateDescriptor<Long>(sn+"_TimerTs", Long.class));
            Long snTimerTs = snTimerValueState.value();
            ValueState<Long> countValueState = runtimeContext.getState(new ValueStateDescriptor<Long>(sn+"_count", Long.class));
            Long count = countValueState.value()==null?0L:countValueState.value();
            Long timeInterval = 0L;
            int  countInterval = 0;
            if (sn.equals("a")) {
                timeInterval = 5000L;
                countInterval = 2;
            }else {
                timeInterval = 10000L;
                countInterval = 3;
            }
            /***
             * 当前数据没有属于的定时窗口就新建一个
             */
            if (null == snTimerTs) {
                countValueState.update(count+1);
                long time = onlineEventTimestamp + timeInterval;
                ctx.timerService().registerEventTimeTimer(time);
                snTimerValueState.update(time);

            }else {
                countValueState.update(count+1);
            }
            if (count >= countInterval) {
                // 达到报警 输出
                out.collect(new SnAlaram(sn,123,"达到报警 输出 报警了", ctx.timestamp(),"0"));
            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<SnAlaram> out) throws Exception {
            // 时间到了 就删除定时器
            String sn = ctx.getCurrentKey();
            ValueState<Long> snTimerValueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>(sn+"_TimerTs", Long.class));
            ValueState<Long> countValueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>(sn+"_count", Long.class));
            Long snTimerTs = snTimerValueState.value();
            ctx.timerService().deleteEventTimeTimer(snTimerTs);
            snTimerValueState.clear();
            countValueState.clear();
            // 恢复
            out.collect(new SnAlaram(sn,123,"恢复", ctx.timestamp(),"1"));
        }
    }



    /***
     * 判断 掉线的依据
     *
     * @param preconditionName
     * @param postconditionName
     * @return
     */
    private static Pattern<SNStatusDto,SNStatusDto> getDisconnectedPattern(String preconditionName,String postconditionName ){
        return Pattern.<SNStatusDto>begin(preconditionName).where(new SimpleCondition<SNStatusDto>() {
            @Override
            public boolean filter(SNStatusDto o) throws Exception {
                return o.getStatusVal().equals("1");
            }
        }).next(postconditionName).where(new SimpleCondition<SNStatusDto>() {
            @Override
            public boolean filter(SNStatusDto o) throws Exception {
                return o.getStatusVal().equals("0");
            }
        }) ;

    }


    /***
     * 掉线流
     */
    static class DisconnectedPatternSelectFunction implements PatternSelectFunction<SNStatusDto, SNDisconnectedDto>{

        /**
         * cep 的前置条件
         */
        private String preconditionName;
        /***
         * cep 的后置添加
         */
        private String postconditionName;


        public DisconnectedPatternSelectFunction(String preconditionName, String postconditionName) {
            this.preconditionName = preconditionName;
            this.postconditionName = postconditionName;
        }

        @Override
        public SNDisconnectedDto select(Map<String, List<SNStatusDto>> pattern) throws Exception {
            SNStatusDto preconditionBean = pattern.get(preconditionName).get(0);
            SNStatusDto postconditionBean = pattern.get(postconditionName).get(0);
            SNDisconnectedDto dto = new SNDisconnectedDto(preconditionBean.getTimestamp(),postconditionBean.getTimestamp());
            dto.setSn(preconditionBean.getSn());
            return dto;
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

    /***
     * 在原始流引出分流 专门处理 掉线问题
     */
    static class SideOutputProcessFunction extends ProcessFunction<SNStatusDto, SNStatusDto> {

        OutputTag<SNStatusDto> disconnectedOutputTag;

        public SideOutputProcessFunction(OutputTag<SNStatusDto> disconnectedOutputTag) {
            this.disconnectedOutputTag = disconnectedOutputTag;
        }

        @Override
        public void processElement(SNStatusDto value, Context ctx, Collector<SNStatusDto> out) throws Exception {
            out.collect(value);
            ctx.output(disconnectedOutputTag,value);
        }
    }

}
