package com.sya.config;

import java.util.Properties;

public class PropertiesUnit {

    private static ResourceLoad loader = ResourceLoad.getInstance();

    public static Properties getProperties(String propName) {
        try {
            return loader.getPropFromProperties(propName);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) {
        Properties properties = PropertiesUnit.getProperties("kafka.properties");
        System.out.println(properties.getProperty("kafka.group.id"));
    }
}
