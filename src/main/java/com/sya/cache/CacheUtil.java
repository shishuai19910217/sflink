package com.sya.cache;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import net.oschina.j2cache.CacheChannel;
import net.oschina.j2cache.CacheObject;
import net.oschina.j2cache.J2Cache;
import net.oschina.j2cache.J2CacheBuilder;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Handler;

@Slf4j
public class CacheUtil implements Serializable {
    private static CacheChannel channel = J2Cache.getChannel();

    public static String getStr(String key){
        CacheObject cacheObject =  channel.get("default",key);
        Object value = cacheObject.getValue();
        return value.toString();
    }

    public static <T> T get(String key){
        CacheObject cacheObject =  channel.get("default",key);
        Object value = cacheObject.getValue();


        return (T)value;
    }

    public static void  set(String key,Object t){
        channel.set("default",key,t);
    }

    public static void  set(Map<String,Object> map){
        channel.set("default",map);
    }
    public static void main(String[] args) {
        channel.set("default","b","sdadasdd");
        long l = System.currentTimeMillis();
        CacheObject cacheObject = channel.get("default", "b");
        Object value = cacheObject.getValue();
        log.info("------------{}redis耗时【{}】----",value,System.currentTimeMillis()-l);
        l = System.currentTimeMillis();
        cacheObject = channel.get("default", "b");
        value = cacheObject.getValue();
        log.info("------------{}耗时【{}】",value,System.currentTimeMillis()-l);

        Map<String,Object> map = new HashMap<String,Object>(16);
        map.put("a","a");
        map.put("b","b");
        channel.set("default",map);

    }

    public static void del(List<String> keys) {
        String[] tmp = new String[keys.size()];
        keys.toArray(tmp);
        channel.evict("default",tmp);
    }
    public static void del(String key) {

        channel.evict("default",key);
    }

    @Data
    static class Dto implements Serializable{
        private String name;
        private String value;

        public Dto(String name, String value) {
            this.name = name;
            this.value = value;
        }
    }
}
