import com.alibaba.druid.pool.DruidDataSource;
import java.sql.*;

public class TestDruidDefault {
    public static void main(String[] args) {
        // 创建 Druid 数据源 - 使用默认配置
        DruidDataSource dataSource = new DruidDataSource();

        // 只配置必要的连接参数
        dataSource.setUrl(
                "jdbc:mysql://127.0.0.1:3307/nyc-taxi?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC");
        dataSource.setUsername("root");
        dataSource.setPassword("calm");
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");

        // 使用 Druid 默认配置,不做任何定制

        try {
            System.out.println("🔄 使用 Druid 默认配置初始化连接池...");
            System.out.println("默认配置:");
            System.out.println("  - initialSize: " + dataSource.getInitialSize());
            System.out.println("  - minIdle: " + dataSource.getMinIdle());
            System.out.println("  - maxActive: " + dataSource.getMaxActive());
            System.out.println("  - testWhileIdle: " + dataSource.isTestWhileIdle());
            System.out.println("  - testOnBorrow: " + dataSource.isTestOnBorrow());
            System.out.println("  - testOnReturn: " + dataSource.isTestOnReturn());
            System.out.println("  - validationQuery: " + dataSource.getValidationQuery());
            System.out.println();

            dataSource.init();
            System.out.println("✅ Druid 连接池初始化成功!");

            // 测试查询
            System.out.println("\n📊 测试查询: SELECT COUNT(*) FROM taxi_trips");
            try (Connection conn = dataSource.getConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM taxi_trips")) {
                if (rs.next()) {
                    System.out.println("✅ 总行数: " + rs.getLong(1));
                }
            }

            System.out.println("\n✅ 所有测试通过!");

        } catch (Exception e) {
            System.err.println("\n❌ 错误:");
            e.printStackTrace();
            System.err.println("\n错误原因分析:");
            if (e.getMessage() != null && e.getMessage().contains("getRows()")) {
                System.err.println("⚠️  Calm 需要支持 MySQL 的服务器变量查询 (如 transaction_isolation)");
                System.err.println("⚠️  这些查询会返回结果集,但 Calm 可能没有正确实现 ResultsetRows");
            } else if (e.getMessage() != null && e.getMessage().contains("queryServerVariable")) {
                System.err.println("⚠️  Calm 需要支持 queryServerVariable 方法");
            }
        } finally {
            // 关闭连接池
            dataSource.close();
            System.out.println("\n🔒 连接池已关闭");
        }
    }
}
