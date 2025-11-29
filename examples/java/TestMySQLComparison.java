import java.sql.*;

public class TestMySQLComparison {
    public static void main(String[] args) {
        testDatabase("MySQL", "jdbc:mysql://127.0.0.1:3306/mysql?useSSL=false&serverTimezone=UTC", "root", "ansjsun");
        System.out.println("\n" + "=".repeat(80) + "\n");
        testDatabase("Calm", "jdbc:mysql://127.0.0.1:3307/nyc-taxi?useSSL=false&serverTimezone=UTC", "root", "calm");
    }
    
    static void testDatabase(String name, String url, String user, String pass) {
        System.out.println("🔍 测试 " + name + ":");
        try {
            Connection conn = DriverManager.getConnection(url, user, pass);
            System.out.println("  ✅ 连接成功");
            
            // 测试 getTransactionIsolation - 这是 Druid 会调用的
            try {
                int level = conn.getTransactionIsolation();
                System.out.println("  ✅ getTransactionIsolation(): " + level);
            } catch (Exception e) {
                System.out.println("  ❌ getTransactionIsolation() 失败: " + e.getMessage());
            }
            
            // 测试直接查询 @@transaction_isolation
            try {
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT @@transaction_isolation");
                if (rs.next()) {
                    System.out.println("  ✅ SELECT @@transaction_isolation: " + rs.getString(1));
                }
                rs.close();
                stmt.close();
            } catch (Exception e) {
                System.out.println("  ❌ SELECT @@transaction_isolation 失败: " + e.getMessage());
            }
            
            conn.close();
        } catch (Exception e) {
            System.out.println("  ❌ 连接失败: " + e.getMessage());
        }
    }
}
