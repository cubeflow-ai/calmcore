import java.sql.*;

public class TestSimpleConnection {
    public static void main(String[] args) {
        String url = "jdbc:mysql://127.0.0.1:3307/nyc-taxi?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";

        try {
            System.out.println("🔄 建立简单连接...");
            Connection conn = DriverManager.getConnection(url, "root", "calm");
            System.out.println("✅ 连接成功!");

            // 测试获取事务隔离级别 - 这是 Druid 初始化时会调用的
            System.out.println("\n🔍 测试获取事务隔离级别...");
            int level = conn.getTransactionIsolation();
            System.out.println("✅ 事务隔离级别: " + level);

            // 测试简单查询
            System.out.println("\n📊 测试查询...");
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM taxi_trips");
            if (rs.next()) {
                System.out.println("✅ 总行数: " + rs.getLong(1));
            }

            conn.close();
            System.out.println("\n✅ 所有测试通过!");

        } catch (Exception e) {
            System.err.println("\n❌ 错误:");
            e.printStackTrace();
        }
    }
}
