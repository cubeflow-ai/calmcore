package org.example.r2dataread;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * MySQL 分页读取示例 - 使用 ORDER BY _nature 优化深度分页
 * 
 * _nature 是 CalmCore 的虚拟字段，表示数据的自然存储顺序（partition -> segment）
 * 使用它可以获得 O(N segments) 的深度分页性能，而不是 O(OFFSET rows)
 */
public class MysqlNaturePagination {
    public static void main(String[] args) {
        String host = "127.0.0.1";
        int port = 3307;
        String database = "r2api";
        String username = "root";
        String password = "";

        String url = String.format(
                "jdbc:mysql://%s:%d/%s?useSSL=false&allowPublicKeyRetrieval=true",
                host, port, database
        );

        System.out.println("连接: " + url);

        try (Connection conn = DriverManager.getConnection(url, username, password)) {
            System.out.println("✅ 连接成功!");

            int pageSize = 10000;  // 每次获取 1 万行
            int offset = 0;
            int totalRows = 0;
            int batchNum = 0;

            long startTime = System.currentTimeMillis();

            while (true) {
                batchNum++;
                
                // 🌿 关键改动：添加 ORDER BY _nature
                // 这会触发 CalmCore 的元数据驱动分页优化
                String sql = String.format(
                        "SELECT * FROM %s ORDER BY _nature LIMIT %d OFFSET %d",
                        database, pageSize, offset
                );

                long batchStart = System.currentTimeMillis();
                
                // ✅ 每次查询创建新的 Statement 和 ResultSet
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(sql)) {

                    int count = 0;
                    // ✅ 只使用 next() 方法,不使用其他导航方法
                    while (rs.next()) {
                        count++;
                        totalRows++;

                        // 只打印前几行避免刷屏
                        if (totalRows <= 5) {
                            System.out.println("应用名字: " + rs.getString("app_name"));
                        }
                    }

                    long batchTime = System.currentTimeMillis() - batchStart;

                    System.out.println(String.format(
                            "✅ Batch %d: fetched %d rows (total: %d) in %d ms",
                            batchNum, count, totalRows, batchTime
                    ));

                    // 如果返回的行数少于 pageSize,说明没有更多数据了
                    if (count < pageSize) {
                        System.out.println("🏁 No more data");
                        break;
                    }

                    offset += pageSize;

                } catch (SQLException e) {
                    System.err.println("❌ 查询出错: " + e.getMessage());
                    e.printStackTrace();
                    break;
                }
            }

            long totalTime = System.currentTimeMillis() - startTime;
            System.out.println(String.format(
                    "✅ Total rows processed: %d in %d ms (avg: %.2f ms/batch)",
                    totalRows, totalTime, (double) totalTime / batchNum
            ));

        } catch (SQLException e) {
            System.err.println("❌ 连接失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
