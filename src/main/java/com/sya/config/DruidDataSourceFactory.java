package com.sya.config;

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.ibatis.datasource.pooled.PooledDataSourceFactory;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DruidDataSourceFactory  extends PooledDataSourceFactory {
    @Override
    public DataSource getDataSource() {
        DruidDataSource druidDataSource = new DruidDataSource();
        druidDataSource.setUrl("jdbc:mysql://47.100.226.41:3306/iot-platform?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=true&serverTimezone=GMT%2B8&rewriteBatchedStatements=true");
        druidDataSource.setUsername("root");
        druidDataSource.setPassword("123456");
        druidDataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
//        ResultSet resultSet = null;
//        try {
//            resultSet = druidDataSource.getConnection().createStatement().executeQuery("select id from machine_device_rel where id=563");
//
//            while (resultSet.next()){
//                String id = resultSet.getString("id");
//                System.out.println("--------------------"+id);
//            }
//        } catch (SQLException throwables) {
//            throwables.printStackTrace();
//        }

        return druidDataSource;
    }
}
