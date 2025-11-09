import java.sql.*;

/**
 * Calm 数据库 JDBC 连接示例
 * 
 * 演示如何通过 JDBC 连接到 Calm 数据库并执行查询操作
 * 
 * 编译运行:
 * javac -cp mysql-connector-java-8.0.33.jar CalmJdbcDemo.java
 * java -cp .:mysql-connector-java-8.0.33.jar CalmJdbcDemo
 * 
 * 或使用 Maven:
 * mvn clean compile exec:java
 */
public class CalmJdbcDemo {
    
    // 数据库连接信息
    private static final String URL = "jdbc:mysql://127.0.0.1:3306/calm?useSSL=false&allowPublicKeyRetrieval=true";
    private static final String USER = "root";    // 可以是任意用户名
    private static final String PASSWORD = "";    // 无需密码
    
    public static void main(String[] args) {
        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║          Calm Database JDBC 连接演示                        ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝\n");
        
        CalmJdbcDemo demo = new CalmJdbcDemo();
        
        try {
            // 1. 测试连接
            demo.testConnection();
            
            // 2. 查看数据库和表
            demo.showDatabasesAndTables();
            
            // 3. 查询所有数据
            demo.queryAllData();
            
            // 4. 条件查询
            demo.queryWithCondition();
            
            // 5. 聚合查询
            demo.aggregateQuery();
            
            System.out.println("\n✅ All tests completed successfully!");
            
        } catch (SQLException e) {
            System.err.println("❌ Database error: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * 测试数据库连接
     */
    public void testConnection() throws SQLException {
        System.out.println("📡 Testing database connection...");
        
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD)) {
            DatabaseMetaData metaData = conn.getMetaData();
            System.out.println("   ✓ Connected to: " + metaData.getDatabaseProductName());
            System.out.println("   ✓ Driver: " + metaData.getDriverName() + " " + metaData.getDriverVersion());
            System.out.println("   ✓ URL: " + URL);
        }
        System.out.println();
    }
    
    /**
     * 显示数据库和表列表
     */
    public void showDatabasesAndTables() throws SQLException {
        System.out.println("📋 Listing databases and tables...");
        
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             Statement stmt = conn.createStatement()) {
            
            // 显示数据库
            System.out.println("\n   Databases:");
            try (ResultSet rs = stmt.executeQuery("SHOW DATABASES")) {
                while (rs.next()) {
                    System.out.println("   - " + rs.getString(1));
                }
            }
            
            // 显示表
            System.out.println("\n   Tables:");
            try (ResultSet rs = stmt.executeQuery("SHOW TABLES")) {
                while (rs.next()) {
                    System.out.println("   - " + rs.getString(1));
                }
            }
        }
        System.out.println();
    }
    
    /**
     * 查询所有数据
     */
    public void queryAllData() throws SQLException {
        System.out.println("📊 Querying all data from 'data' table...");
        
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM data LIMIT 10")) {
            
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            
            // 打印表头
            System.out.print("\n   ");
            for (int i = 1; i <= columnCount; i++) {
                System.out.printf("%-15s", metaData.getColumnName(i));
            }
            System.out.println();
            System.out.println("   " + "─".repeat(15 * columnCount));
            
            // 打印数据行
            int rowCount = 0;
            while (rs.next()) {
                System.out.print("   ");
                for (int i = 1; i <= columnCount; i++) {
                    String value = rs.getString(i);
                    System.out.printf("%-15s", value != null ? value : "NULL");
                }
                System.out.println();
                rowCount++;
            }
            
            System.out.println("\n   Total rows: " + rowCount);
        }
        System.out.println();
    }
    
    /**
     * 带条件的查询 (使用 PreparedStatement)
     */
    public void queryWithCondition() throws SQLException {
        System.out.println("🔍 Querying data with condition (age > 25)...");
        
        String query = "SELECT * FROM data WHERE age > ? LIMIT 10";
        
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             PreparedStatement pstmt = conn.prepareStatement(query)) {
            
            pstmt.setInt(1, 25);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                System.out.print("\n   ");
                System.out.printf("%-10s %-20s %-10s%n", "ID", "Name", "Age");
                System.out.println("   " + "─".repeat(42));
                
                int count = 0;
                while (rs.next()) {
                    long id = rs.getLong("id");
                    String name = rs.getString("name");
                    int age = rs.getInt("age");
                    
                    System.out.print("   ");
                    System.out.printf("%-10d %-20s %-10d%n", id, name, age);
                    count++;
                }
                
                System.out.println("\n   Found " + count + " rows where age > 25");
            }
        }
        System.out.println();
    }
    
    /**
     * 聚合查询
     */
    public void aggregateQuery() throws SQLException {
        System.out.println("📈 Running aggregate queries...");
        
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             Statement stmt = conn.createStatement()) {
            
            // COUNT
            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as total FROM data")) {
                if (rs.next()) {
                    System.out.println("   Total records: " + rs.getLong("total"));
                }
            }
            
            // AVG
            try (ResultSet rs = stmt.executeQuery("SELECT AVG(age) as avg_age FROM data")) {
                if (rs.next()) {
                    System.out.printf("   Average age: %.2f%n", rs.getDouble("avg_age"));
                }
            }
            
            // MIN/MAX
            try (ResultSet rs = stmt.executeQuery("SELECT MIN(age) as min_age, MAX(age) as max_age FROM data")) {
                if (rs.next()) {
                    System.out.println("   Age range: " + rs.getInt("min_age") + " - " + rs.getInt("max_age"));
                }
            }
        }
        System.out.println();
    }
    
    /**
     * 批量插入示例 (需要完整的 DML 支持后使用)
     * 
     * 当前版本不支持,需要使用 Rust API
     */
    @SuppressWarnings("unused")
    public void batchInsert() throws SQLException {
        System.out.println("⚠️  INSERT operations are not yet supported through JDBC");
        System.out.println("   Please use Calm Rust API: partition.upsert(data)");
        
        // 未来版本将支持:
        // String sql = "INSERT INTO users (id, name, age) VALUES (?, ?, ?)";
        // try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
        //      PreparedStatement pstmt = conn.prepareStatement(sql)) {
        //     
        //     pstmt.setLong(1, 1001);
        //     pstmt.setString(2, "Alice");
        //     pstmt.setInt(3, 30);
        //     pstmt.executeUpdate();
        // }
    }
}
