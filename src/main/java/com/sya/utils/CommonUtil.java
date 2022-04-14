package com.sya.utils;


import java.util.Collection;
import java.util.Map;

/***
 * 1.禁止出现业务代码
 * 2.推荐使用单例模式(构造方法 必须是私有的)
 * 3.方法声明必须是static
 * 4.方法注释必须包含:功能,入参，出参解释，（复杂方法必须包含示例） 注释要尽量的详细
 * 5.尽量避免使用Map，Json这些复杂的数据对象作为参数和结果
 * 6.类注释必须包含当前工具类处理数据的类型 示例：主要是对string转换的工具类
 * 7.类名必须以Util结尾
 * 8.方法体的代码行数不等超过200
 * 9.import列表不得引用其他模块类（第三方导入包除外）
 * 10.必须保证工具类的单一性 示例：在StringUtil中禁止出现了 date格式的转换
 * 11.如果 当前工具类只引入了jdk的原生类那么当前类可以直接放在cn.usr.util包下
 * 12.如果当前工具类是其他第三方工具类的一个扩展或者是引入了第三方包当前类要以功能分类创建包路径 示例EhcacheUtil
 * EhcacheUtil主要实现了缓存相关的功能并且引用了Ehcache相关的包 就需要放在cn.usr.util.cache包下
 *
 * @author
 */
public final class CommonUtil {
    /***
     * 判空,有一个为null就为真 不能对数组判空!!!
     * 因为 可变参数本身就是一个数组了
     * 主要对 普通对象，集合，数值类型判空(不能对数组判空)
     * @param args 需要判空的对象
     * @return 返回判断结果
     */
    public static boolean judgeEmpty(Object... args) {
        try {
            for (Object obj : args) {
                if (obj == null) {
                    return true;
                }
                if (obj.getClass() == String.class && (String.valueOf(obj)).length() == 0) {
                    return true;
                }
                if (obj instanceof Collection && (obj == null || ((Collection) obj).size() == 0)) {
                    return true;
                }
                if (obj instanceof Map && (obj == null || ((Map) obj).size() == 0)) {
                    return true;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            return true;
        }
        return false;
    }

    public static Double str2Double(String numStr){
        if (CommonUtil.judgeEmpty(numStr.trim())) {
            return 0D;
        }
        double data = Double.parseDouble(numStr.trim());
        return data;
    }

    public static void main(String[] args) {
        System.out.println(CommonUtil.str2Double("123").doubleValue()==CommonUtil.str2Double("123.00"));

    }

}
