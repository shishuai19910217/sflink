package com.sya.kafka;

import com.sya.dto.SNStatusDto;
import com.sya.dto.SnAlaram;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class KeyedProcessNoDataJobWithProcessTime {
    public static void main(String[] args) throws Exception {
        // 获取 flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("localhost", 9999);
        //env.setParallelism(1);
        // 设置状态后端
        OutputTag<SnAlaram> alramTag = new OutputTag<SnAlaram>("sn-alarm"){};
        DataStream<SNStatusDto> mapStream = stringDataStreamSource.map(new SNStatusMapper());
        KeyedStream<SNStatusDto, String> keyedStream = mapStream.keyBy(new KeySelector<SNStatusDto, String>() {
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
     * 报警频率 推送消息使用
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
            // 查看配置是不是仅推送一次
            boolean onlyStatus = true;

            // 定时任务
            ValueState<Long> snTimerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>(sn+"_alramTimerTs", Long.class));
            Long snTimerTsVal = snTimerTs.value();
            // 报警中
            if ("0".equals(status)) {
                if (onlyStatus) {
                    // 输出报警信息
                    System.out.println(sn+"---报警了---"+ctx.timerService().currentProcessingTime());
                }else {
                    if (null == snTimerTsVal) {
                        // 计算出 定时任务的时间戳
                        long ts = ctx.timerService().currentProcessingTime();
                        ts = ts + timeInterval; // 五秒
                        ctx.timerService().registerProcessingTimeTimer(ts);
                        System.out.println("----所属的时间窗是---"+ts);
                        snTimerTs.update(ts);
                    }else {
                        System.out.println("----所属的时间窗是---"+snTimerTsVal);
                    }
                }

            }else {
                // 删除 相关定时任务
                // 计算出 定时任务的时间戳
                if (null != snTimerTsVal) {
                    ctx.timerService().deleteProcessingTimeTimer(snTimerTsVal);
                    snTimerTs.clear();
                }

            }
        }
    }

    /***
     * 将原始流 拆出 N分钟不在线的处理流
     */
    static class SingleOutKeyedProcessFunction extends KeyedProcessFunction<String, SNStatusDto, SNStatusDto> {

        private  OutputTag<SnAlaram> alramTag;


        public SingleOutKeyedProcessFunction(OutputTag<SnAlaram> alramTag) {
            this.alramTag = alramTag;
        }
        @Override
        public void processElement(SNStatusDto value, Context ctx, Collector<SNStatusDto> out) throws Exception {
            String statusVal = value.getStatusVal();
            String sn = value.getSn();
            // 定时任务
            ValueState<Long> snTimerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>(sn+"_TimerTs", Long.class));
            Long snTimerTsVal = snTimerTs.value();
            // 定时任务的长度，次数限制  配置获取
            Long timeInterval = 0L;
            int offCountInterval = 0;
            if (sn.equals("a")) {
                timeInterval = 5000L;
                offCountInterval = 2;
            }else {
                timeInterval = 10000L;
                offCountInterval = 3;
            }
            Integer ruleId = 14;
            // 记录的离线次数
            ValueState<Long> offCountState = getRuntimeContext().getState(new ValueStateDescriptor<Long>(sn + "_offCount", Long.class));
            long offCount = offCountState.value() == null ? 0 : offCountState.value();
            // 是否已经发过报警信息
            ValueState<Boolean> sendAlarmStatusState =  getRuntimeContext().getState(new ValueStateDescriptor<Boolean>(sn + "_sendAlarmStatus", Boolean.class));
            boolean sendAlarmStatus = sendAlarmStatusState.value() == null ? false : true;
            long currentProcessingTime = ctx.timerService().currentProcessingTime();

            // 掉线状态
            if ("0".equals(statusVal)) {
                if (null == snTimerTsVal) {
                    // 其实新建定时的时候 次数肯定是1
                    offCount = 1;
                    offCountState.update(offCount);
                    // 注册定时器
                    long time = currentProcessingTime + timeInterval;
                    ctx.timerService().registerProcessingTimeTimer(time);
                    snTimerTs.update(time);
                }else {
                    // 说明在某个定时窗口中
                    offCount = offCount + 1;
                    // 超出次数阈值就报警
                    if (offCount >= offCountInterval) {
                        // 报警了
                        if (!sendAlarmStatus) {

                            ctx.output(alramTag,new SnAlaram(sn,ruleId,"报警了",currentProcessingTime,"0"));
                        }
                    }


                }
            }else {
                // 说明 上个定时任务已经到期
                if (null == snTimerTsVal) {
                    ctx.output(alramTag,new SnAlaram(sn,ruleId,"报警解除",currentProcessingTime,"1"));
                }
            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<SNStatusDto> out) throws Exception {
            // 其实就是数据入库
            String sn = ctx.getCurrentKey();
            ValueState<Long> snTimerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>(sn+"_TimerTs", Long.class));
            Long snTimerTsVal = snTimerTs.value();
            // 是否已经发过报警信息
            ValueState<Boolean> sendAlarmStatusState =  getRuntimeContext().getState(new ValueStateDescriptor<Boolean>(sn + "_sendAlarmStatus", Boolean.class));

            ValueState<Long> offCountState = getRuntimeContext().getState(new ValueStateDescriptor<Long>(sn + "_offCount", Long.class));
            // 时间到了删除本定时器
            ctx.timerService().deleteProcessingTimeTimer(snTimerTsVal);
            snTimerTs.clear();
            offCountState.clear();
            sendAlarmStatusState.clear();
        }
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            //
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
