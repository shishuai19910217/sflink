package com.sya.test;

import lombok.Data;

import java.io.Serializable;
@Data
public class Dto implements Serializable {
    private String sn;
    private String val;
}
