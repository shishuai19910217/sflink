package com.sya.config;

import com.alibaba.fastjson.JSONObject;
import com.sya.mapper.MachineDeviceRelMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;

@Slf4j
public class MybatisConfig {

    //创建ThreadLocal绑定当前线程中的SqlSession对象
    private static final ThreadLocal<SqlSession> tl = new ThreadLocal<SqlSession>();
    private static DruidDataSourceFactory druidDataSourceFactory = new DruidDataSourceFactory();
    private static SqlSessionFactory sqlSessionFactory;

    static {
        TransactionFactory transactionFactory = new JdbcTransactionFactory();
        Environment environment = new Environment("development", transactionFactory,
                druidDataSourceFactory.getDataSource());

        Configuration configuration = new Configuration(environment);
        configuration.addMappers("com.sya.mapper");
       // configuration.setLogImpl(Log4j2Impl.class);
        sqlSessionFactory = new SqlSessionFactoryBuilder().build(configuration);
    }

    //获得连接（从tl中获得当前线程SqlSession）
    private static SqlSession openSession(boolean AutoCommit){
        SqlSession session = tl.get();
        if(session == null){
            session = sqlSessionFactory.openSession(AutoCommit);
            tl.set(session);
        }
        return session;
    }

    //提供设置手动提交的sqlsession对象（如果需要手动提交事务，则调用该方法）
    public static SqlSession getSqlSession(){
        return openSession(false);
    }

    //获得接口实现类对象（如果通过该方法获取实现类对象，则表示需要自动提交事务）
    public static <T extends Object> T getMapper(Class<T> clazz){
        SqlSession session = openSession(true);
        return session.getMapper(clazz);
    }
    public static void main(String[] args) {
        MachineDeviceRelMapper mapper = MybatisConfig.getMapper(MachineDeviceRelMapper.class);
        MachineDeviceRel list = mapper.getList(537);
        log.info("aaaaa--------------{}", JSONObject.toJSONString(list));
    }
}
