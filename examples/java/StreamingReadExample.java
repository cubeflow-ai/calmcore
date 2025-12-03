import java.sql.*;

/**
 * Calm 数据库流式读取示例
 * 
 * 演示如何使用 JDBC 流式游标高效读取大结果集
 * 避免一次性加载所有数据到内存
 */
public class StreamingReadExample {
    
    public static void main(String[] args) {
        String url = "jdbc:mysql://localhost:3306/calm";
        String user = "root";
        String password = "password";
        
        // 示例 1: 基础流式读取
        basicStreamingRead(url, user, password);
        
        // 示例 2: 带进度显示的流式读取
        streamingReadWithProgress(url, user, password);
        
        // 示例 3: 分页读取（LIMIT + OFFSET）
        paginatedRead(url, user, password);
        
        // 示例 4: 游标流式读取（最高效）
        cursorStreamingRead(url, user, password);
    }
    
    /**
     * 示例 1: 基础流式读取
     * 使用 setFetchSize 控制每次从服务器获取的行数
     */
    public static void basicStreamingRead(String url, String user, String password) {
        System.out.println("\n========== 示例 1: 基础流式读取 ==========");
        
        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            String sql = "SELECT * FROM r2api LIMIT 10000";
            
            try (Statement stmt = conn.createStatement()) {
                // 🔑 关键: 设置 fetchSize，每次从服务器拉取 1000 行
                // 这样客户端不会一次性加载全部数据到内存
                stmt.setFetchSize(1000);
                
                System.out.println("📊 执行查询: " + sql);
                System.out.println("📦 FetchSize: " + stmt.getFetchSize());
                
                long startTime = System.currentTimeMillis();
                ResultSet rs = stmt.executeQuery(sql);
                
                int count = 0;
                while (rs.next()) {
                    // 处理每一行数据
                    String clientIp = rs.getString("client_ip");
                    String r2Key = rs.getString("r2_key");
                    
                    count++;
                    
                    // 每 1000 行打印一次进度
                    if (count % 1000 == 0) {
                        System.out.printf("  处理了 %d 行...\n", count);
                    }
                    
                    // 模拟数据处理
                    // processRow(clientIp, r2Key);
                }
                
                long elapsed = System.currentTimeMillis() - startTime;
                System.out.printf("✅ 完成: 读取 %d 行，耗时 %d ms\n", count, elapsed);
                
            }
        } catch (SQLException e) {
            System.err.println("❌ 错误: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * 示例 2: 带进度显示的流式读取
     * 显示读取速度和预计剩余时间
     */
    public static void streamingReadWithProgress(String url, String user, String password) {
        System.out.println("\n========== 示例 2: 带进度显示的流式读取 ==========");
        
        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            String sql = "SELECT * FROM r2api LIMIT 50000";
            
            try (Statement stmt = conn.createStatement()) {
                stmt.setFetchSize(1000);
                
                System.out.println("📊 执行查询: " + sql);
                
                long startTime = System.currentTimeMillis();
                ResultSet rs = stmt.executeQuery(sql);
                
                int count = 0;
                long lastReportTime = startTime;
                
                while (rs.next()) {
                    count++;
                    
                    // 每 5000 行打印详细进度
                    if (count % 5000 == 0) {
                        long now = System.currentTimeMillis();
                        long elapsed = now - startTime;
                        long intervalTime = now - lastReportTime;
                        
                        double speed = 5000.0 / (intervalTime / 1000.0); // 行/秒
                        double avgSpeed = count / (elapsed / 1000.0);
                        
                        System.out.printf(
                            "  📈 进度: %d 行 | 速度: %.0f 行/秒 | 平均: %.0f 行/秒 | 耗时: %.1f 秒\n",
                            count, speed, avgSpeed, elapsed / 1000.0
                        );
                        
                        lastReportTime = now;
                    }
                }
                
                long elapsed = System.currentTimeMillis() - startTime;
                double avgSpeed = count / (elapsed / 1000.0);
                System.out.printf("✅ 完成: %d 行，耗时 %.1f 秒，平均速度 %.0f 行/秒\n", 
                    count, elapsed / 1000.0, avgSpeed);
                
            }
        } catch (SQLException e) {
            System.err.println("❌ 错误: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * 示例 3: 分页读取
     * 使用 LIMIT + OFFSET 方式，适合需要跳页的场景
     */
    public static void paginatedRead(String url, String user, String password) {
        System.out.println("\n========== 示例 3: 分页读取 (LIMIT + OFFSET) ==========");
        
        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            int pageSize = 1000;
            int totalRead = 0;
            int page = 0;
            
            while (true) {
                int offset = page * pageSize;
                String sql = String.format(
                    "SELECT * FROM r2api LIMIT %d OFFSET %d", 
                    pageSize, offset
                );
                
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(sql)) {
                    
                    int rowsInPage = 0;
                    while (rs.next()) {
                        rowsInPage++;
                        totalRead++;
                        
                        // 处理数据
                        String clientIp = rs.getString("client_ip");
                        String r2Key = rs.getString("r2_key");
                    }
                    
                    if (rowsInPage > 0) {
                        System.out.printf("  📄 第 %d 页: %d 行 (offset=%d)\n", 
                            page + 1, rowsInPage, offset);
                    }
                    
                    // 如果这页没有数据，说明读完了
                    if (rowsInPage == 0) {
                        break;
                    }
                    
                    page++;
                    
                    // 示例：只读 5 页
                    if (page >= 5) {
                        System.out.println("  ⏹️  示例限制：只读 5 页");
                        break;
                    }
                }
            }
            
            System.out.printf("✅ 完成: 共读取 %d 行，%d 页\n", totalRead, page);
            
        } catch (SQLException e) {
            System.err.println("❌ 错误: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * 示例 4: 游标流式读取（MySQL 5.0+）
     * 最高效的方式，使用服务器端游标
     */
    public static void cursorStreamingRead(String url, String user, String password) {
        System.out.println("\n========== 示例 4: 游标流式读取 (SERVER CURSOR) ==========");
        
        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            String sql = "SELECT * FROM r2api LIMIT 100000";
            
            // 🔑 关键配置: 使用服务器端游标
            // 需要 PreparedStatement + TYPE_FORWARD_ONLY + CONCUR_READ_ONLY + setFetchSize(Integer.MIN_VALUE)
            try (PreparedStatement pstmt = conn.prepareStatement(
                    sql,
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY)) {
                
                // 🔑 MySQL 特殊值: Integer.MIN_VALUE 启用流式结果集
                pstmt.setFetchSize(Integer.MIN_VALUE);
                
                System.out.println("📊 执行查询: " + sql);
                System.out.println("🎯 使用服务器端游标（最高效）");
                
                long startTime = System.currentTimeMillis();
                ResultSet rs = pstmt.executeQuery();
                
                int count = 0;
                long lastReportTime = startTime;
                
                while (rs.next()) {
                    // 处理数据
                    String clientIp = rs.getString("client_ip");
                    String r2Key = rs.getString("r2_key");
                    String database = rs.getString("database");
                    
                    count++;
                    
                    // 每 10000 行打印进度
                    if (count % 10000 == 0) {
                        long now = System.currentTimeMillis();
                        long elapsed = now - startTime;
                        double speed = count / (elapsed / 1000.0);
                        
                        System.out.printf(
                            "  📈 进度: %d 行 | 速度: %.0f 行/秒 | 耗时: %.1f 秒\n",
                            count, speed, elapsed / 1000.0
                        );
                    }
                    
                    // 模拟慢速消费（测试背压）
                    // if (count % 1000 == 0) {
                    //     Thread.sleep(100);
                    // }
                }
                
                long elapsed = System.currentTimeMillis() - startTime;
                double avgSpeed = count / (elapsed / 1000.0);
                System.out.printf("✅ 完成: %d 行，耗时 %.1f 秒，平均速度 %.0f 行/秒\n", 
                    count, elapsed / 1000.0, avgSpeed);
                
            }
        } catch (SQLException e) {
            System.err.println("❌ 错误: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * 工具方法: 处理单行数据
     */
    private static void processRow(String clientIp, String r2Key) {
        // 实际的业务逻辑
        // 例如: 数据转换、写入另一个数据库、生成报表等
    }
    
    /**
     * 示例 5: 内存监控版流式读取
     * 监控 JVM 内存使用情况，验证流式读取的内存效率
     */
    public static void streamingReadWithMemoryMonitor(String url, String user, String password) {
        System.out.println("\n========== 示例 5: 内存监控版流式读取 ==========");
        
        Runtime runtime = Runtime.getRuntime();
        
        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            String sql = "SELECT * FROM r2api LIMIT 100000";
            
            try (PreparedStatement pstmt = conn.prepareStatement(
                    sql,
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY)) {
                
                pstmt.setFetchSize(Integer.MIN_VALUE);
                
                long startMemory = runtime.totalMemory() - runtime.freeMemory();
                System.out.printf("🧠 初始内存: %.2f MB\n", startMemory / 1024.0 / 1024.0);
                
                long startTime = System.currentTimeMillis();
                ResultSet rs = pstmt.executeQuery();
                
                int count = 0;
                long maxMemory = startMemory;
                
                while (rs.next()) {
                    count++;
                    
                    if (count % 10000 == 0) {
                        long currentMemory = runtime.totalMemory() - runtime.freeMemory();
                        maxMemory = Math.max(maxMemory, currentMemory);
                        
                        System.out.printf(
                            "  📊 %d 行 | 当前内存: %.2f MB | 峰值: %.2f MB | 增长: %.2f MB\n",
                            count,
                            currentMemory / 1024.0 / 1024.0,
                            maxMemory / 1024.0 / 1024.0,
                            (currentMemory - startMemory) / 1024.0 / 1024.0
                        );
                    }
                }
                
                long endMemory = runtime.totalMemory() - runtime.freeMemory();
                long elapsed = System.currentTimeMillis() - startTime;
                
                System.out.printf("✅ 完成: %d 行\n", count);
                System.out.printf("🧠 结束内存: %.2f MB\n", endMemory / 1024.0 / 1024.0);
                System.out.printf("📈 峰值内存: %.2f MB\n", maxMemory / 1024.0 / 1024.0);
                System.out.printf("📊 内存增长: %.2f MB\n", (maxMemory - startMemory) / 1024.0 / 1024.0);
                System.out.printf("⏱️  耗时: %.1f 秒\n", elapsed / 1000.0);
            }
        } catch (SQLException e) {
            System.err.println("❌ 错误: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
