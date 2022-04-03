//package com.sya.cache;
//
//import lombok.Data;
//import net.oschina.j2cache.CacheChannel;
//import net.oschina.j2cache.CacheObject;
//import net.oschina.j2cache.J2Cache;
//
//import java.io.Serializable;
//import java.util.HashMap;
//import java.util.Map;
//
//public class CacheUtil implements Serializable {
//    private static CacheChannel channel = J2Cache.getChannel();
//
//    public static String getStr(String key){
//        CacheObject cacheObject =  channel.get("default",key);
//        Object value = cacheObject.getValue();
//        return value.toString();
//    }
//
//    public static <T> T get(String key){
//        CacheObject cacheObject =  channel.get("default",key);
//        Object value = cacheObject.getValue();
//
//        return (T)value;
//    }
//
//    public static void  set(String key,Object t){
//        channel.set("default",key,t);
//    }
//
//    public static void main(String[] args) {
//        System.out.printf("", channel.get("default", "b").getValue());
//
//    }
//
//    @Data
//    static class Dto implements Serializable{
//        private String name;
//        private String value;
//
//        public Dto(String name, String value) {
//            this.name = name;
//            this.value = value;
//        }
//    }
//}
