import com.alibaba.druid.pool.DruidDataSource;
import java.sql.*;

public class TestDruidConnection {
    public static void main(String[] args) {
        // 创建 Druid 数据源
        DruidDataSource dataSource = new DruidDataSource();

        // 配置连接参数
        dataSource.setUrl("jdbc:mysql://127.0.0.1:3307/?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC");
        dataSource.setUsername("root");
        dataSource.setPassword("calm");
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");

        // 配置连接池参数
        dataSource.setInitialSize(1); // 初始连接数
        dataSource.setMinIdle(1); // 最小空闲连接
        dataSource.setMaxActive(10); // 最大活跃连接
        dataSource.setMaxWait(60000); // 获取连接最大等待时间(ms)

        // 配置检测参数
        dataSource.setTestWhileIdle(false); // 关闭空闲检测,避免触发服务器变量查询
        dataSource.setTestOnBorrow(false); // 获取时不检测
        dataSource.setTestOnReturn(false); // 归还时不检测
        // dataSource.setValidationQuery("SELECT 1"); // 注释掉验证查询

        // 配置超时参数
        dataSource.setTimeBetweenEvictionRunsMillis(60000); // 检测间隔
        dataSource.setMinEvictableIdleTimeMillis(300000); // 最小空闲时间

        // 添加连接属性,禁用一些可能导致问题的 MySQL 特性
        dataSource.addConnectionProperty("useInformationSchema", "true");
        dataSource.addConnectionProperty("useLocalSessionState", "true");

        try {
            System.out.println("🔄 初始化 Druid 连接池...");
            dataSource.init();
            System.out.println("✅ Druid 连接池初始化成功!");

            // 测试1: 简单计数查询
            System.out.println("\n📊 测试1: SELECT COUNT(*) FROM taxi_trips");
            try (Connection conn = dataSource.getConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM taxi_trips")) {
                if (rs.next()) {
                    System.out.println("✅ 总行数: " + rs.getLong(1));
                }
            }

            // 测试2: 带 WHERE 条件的查询
            System.out.println("\n📊 测试2: 带 WHERE 条件");
            String sql2 = "SELECT pickup_location_id, COUNT(*) as cnt " +
                    "FROM taxi_trips " +
                    "WHERE passenger_count = 1 " +
                    "GROUP BY pickup_location_id " +
                    "ORDER BY cnt DESC " +
                    "LIMIT 5";
            try (Connection conn = dataSource.getConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(sql2)) {
                System.out.println("Location ID | Count");
                System.out.println("------------|-------");
                while (rs.next()) {
                    System.out.printf("%11d | %d%n",
                            rs.getInt("pickup_location_id"),
                            rs.getLong("cnt"));
                }
            }

            // 测试3: PreparedStatement
            System.out.println("\n📊 测试3: PreparedStatement 参数化查询");
            String sql3 = "SELECT COUNT(*) FROM taxi_trips WHERE passenger_count = ?";
            try (Connection conn = dataSource.getConnection();
                    PreparedStatement pstmt = conn.prepareStatement(sql3)) {
                pstmt.setInt(1, 2);
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) {
                        System.out.println("✅ passenger_count=2 的行数: " + rs.getLong(1));
                    }
                }
            }

            // 测试4: GROUP BY with ORDER BY
            System.out.println("\n📊 测试4: GROUP BY with ORDER BY ASC");
            String sql4 = "SELECT pickup_location_id, COUNT(*) as cnt " +
                    "FROM taxi_trips " +
                    "GROUP BY pickup_location_id " +
                    "ORDER BY cnt ASC " +
                    "LIMIT 5";
            try (Connection conn = dataSource.getConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(sql4)) {
                System.out.println("Location ID | Count (ASC)");
                System.out.println("------------|------------");
                while (rs.next()) {
                    System.out.printf("%11d | %d%n",
                            rs.getInt("pickup_location_id"),
                            rs.getLong("cnt"));
                }
            }

            // 显示连接池统计
            System.out.println("\n📈 连接池统计:");
            System.out.println("活跃连接数: " + dataSource.getActiveCount());
            System.out.println("空闲连接数: " + dataSource.getPoolingCount());
            System.out.println("总连接数: " + (dataSource.getActiveCount() + dataSource.getPoolingCount()));

        } catch (Exception e) {
            System.err.println("❌ 错误:");
            e.printStackTrace();
        } finally {
            // 关闭连接池
            dataSource.close();
            System.out.println("\n🔒 连接池已关闭");
        }
    }
}
