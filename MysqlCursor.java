import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MysqlCursor {
    public static void main(String[] args) {
        String host = "127.0.0.1";
        int port = 3307;
        String database = "r2api";
        String username = "root";
        String password = "";
        int fetchSize = 1000;
        
        // 尝试多个不同的连接字符串配置
        String[] urls = {
            // 配置 1: 基础配置
            String.format(
                "jdbc:mysql://%s:%d/%s?useSSL=false&allowPublicKeyRetrieval=true",
                host, port, database
            ),
            
            // 配置 2: 添加更多兼容参数
            String.format(
                "jdbc:mysql://%s:%d/%s?useSSL=false&allowPublicKeyRetrieval=true&autoReconnect=true&useUnicode=true&characterEncoding=UTF-8",
                host, port, database
            ),
            
            // 配置 3: 禁用所有安全检查
            String.format(
                "jdbc:mysql://%s:%d/%s?useSSL=false&requireSSL=false&verifyServerCertificate=false&allowPublicKeyRetrieval=true&enabledTLSProtocols=TLSv1.2",
                host, port, database
            ),
            
            // 配置 4: 使用旧协议
            String.format(
                "jdbc:mysql://%s:%d/%s?useSSL=false&allowPublicKeyRetrieval=true&useOldAliasMetadataBehavior=true&zeroDateTimeBehavior=convertToNull",
                host, port, database
            )
        };
        
        for (int i = 0; i < urls.length; i++) {
            System.out.println("\n========================================");
            System.out.println("尝试配置 " + (i + 1) + ":");
            System.out.println(urls[i]);
            System.out.println("========================================");
            
            try (Connection conn = DriverManager.getConnection(urls[i], username, password)) {
                System.out.println("✅ 连接成功!");
                
                // 测试简单查询
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + database)) {
                    if (rs.next()) {
                        System.out.println("表行数: " + rs.getLong(1));
                    }
                }
                
                System.out.println("✅ 查询成功! 使用此配置");
                return; // 成功就退出
                
            } catch (SQLException e) {
                System.out.println("❌ 失败: " + e.getMessage());
                System.out.println("错误代码: " + e.getErrorCode());
                System.out.println("SQL状态: " + e.getSQLState());
                
                // 打印详细错误
                Throwable cause = e.getCause();
                if (cause != null) {
                    System.out.println("根本原因: " + cause.getMessage());
                }
            }
        }
        
        System.out.println("\n所有配置都失败了。需要检查服务器端实现。");
    }
}
