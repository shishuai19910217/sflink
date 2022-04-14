package com.sya.config;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ResourceLoad {
    private static ResourceLoad loader = new ResourceLoad();
    private static Map<String, Properties> loaderMap = new HashMap<String, Properties>();

    private ResourceLoad() {
    }

    public static ResourceLoad getInstance() {
        return loader;
    }

    public Properties getPropFromProperties(String fileName) throws Exception {

        Properties prop = loaderMap.get(fileName);
        if (prop != null) {
            return prop;
        }
        Properties properties = new Properties();
        properties.load(this.getClass().getClassLoader().getResourceAsStream(fileName));
        loaderMap.put(fileName, properties);
        return properties;
    }
}
