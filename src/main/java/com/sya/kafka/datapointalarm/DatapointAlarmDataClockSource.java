package com.sya.kafka.datapointalarm;

import com.sya.dto.ClockDto;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.ArrayList;
import java.util.List;

/***
 * 定义一个时钟
 */
public class DatapointAlarmDataClockSource extends RichParallelSourceFunction<ClockDto> {
    private static List<String> sns = new ArrayList<>();
    static {
        sns.add("a");
//        sns.add("b");
//        sns.add("c");
//        sns.add("d");
//        sns.add("e");
//        sns.add("f");

    }
    @Override
    public void run(SourceContext<ClockDto> ctx) throws Exception {
       int i = 0;
        while (true) {
           Thread.sleep(1000);

            long timestamp = System.currentTimeMillis();
            Long l = timestamp % sns.size();
            String s = sns.get(l.intValue());

            ctx.collect(new ClockDto(timestamp,"0",s));
        }
    }

    @Override
    public void cancel() {

    }
}
