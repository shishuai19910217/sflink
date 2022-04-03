package com.sya.kafka.source;

import com.alibaba.fastjson.JSONObject;
import dto.ClockDto;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;

/***
 * 定义一个时钟
 */
public class MySensorSource extends RichParallelSourceFunction<ClockDto> {
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
    public void run(SourceFunction.SourceContext<ClockDto> ctx) throws Exception {
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