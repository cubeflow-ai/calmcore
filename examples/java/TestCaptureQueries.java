import java.sql.*;

public class TestCaptureQueries {
    public static void main(String[] args) {
        // 启用详细日志
        System.setProperty("com.mysql.cj.log.Log", "com.mysql.cj.log.StandardLogger");
        System.setProperty("com.mysql.cj.log.StandardLogger.level", "DEBUG");

        String url = "jdbc:mysql://127.0.0.1:3307/nyc-taxi?"
                + "useSSL=false"
                + "&allowPublicKeyRetrieval=true"
                + "&serverTimezone=UTC"
                + "&logger=StandardLogger"
                + "&profileSQL=true"
                + "&maxQuerySizeToLog=10000";

        try {
            System.out.println("=".repeat(80));
            System.out.println("🔄 建立连接...");
            System.out.println("=".repeat(80));
            Connection conn = DriverManager.getConnection(url, "root", "calm");
            System.out.println("✅ 连接成功!");

            System.out.println("\n" + "=".repeat(80));
            System.out.println("🔍 调用 getTransactionIsolation()...");
            System.out.println("=".repeat(80));
            int level = conn.getTransactionIsolation();
            System.out.println("✅ 事务隔离级别: " + level);

            conn.close();
            System.out.println("\n✅ 测试完成!");

        } catch (Exception e) {
            System.err.println("\n" + "=".repeat(80));
            System.err.println("❌ 错误:");
            System.err.println("=".repeat(80));
            e.printStackTrace();
        }
    }
}
