import com.alibaba.druid.pool.DruidDataSource;
import java.sql.*;
import java.util.logging.*;

public class TestDruidDebug {
    public static void main(String[] args) {
        // 启用 MySQL JDBC 日志
        System.setProperty("com.mysql.cj.log.Log", "com.mysql.cj.log.StandardLogger");
        System.setProperty("com.mysql.cj.log.StandardLogger.level", "TRACE");
        
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setUrl("jdbc:mysql://127.0.0.1:3307/nyc-taxi?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC&logger=StandardLogger&profileSQL=true");
        dataSource.setUsername("root");
        dataSource.setPassword("calm");
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        
        try {
            System.out.println("🔄 初始化连接...");
            dataSource.init();
            System.out.println("✅ 成功!");
        } catch (Exception e) {
            System.err.println("❌ 错误:");
            e.printStackTrace();
        } finally {
            dataSource.close();
        }
    }
}
