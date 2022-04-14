package com.sya.dto;

import java.io.Serializable;

public class WInData implements Serializable {
    private long time;
    private String val;
    private String name;
    private Long start;
    private long end;
    // 正在报警
    private Integer status;

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public Long getStart() {
        return start;
    }

    public void setStart(Long start) {
        this.start = start;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public WInData(){}
    public WInData(long time, String val) {
        this.time = time;
        this.val = val;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public String getVal() {
        return val;
    }

    public void setVal(String val) {
        this.val = val;
    }

    @Override
    public String toString() {
        return "WInData{" +
                "time=" + time +
                ", val='" + val + '\'' +
                ", name='" + name + '\'' +
                ", start=" + start +
                ", end=" + end +
                '}';
    }
}
