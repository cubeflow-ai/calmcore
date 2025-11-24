#!/bin/bash

echo "========================================"
echo "测试 MySQL JDBC 连接"
echo "========================================"

# 检查 Java 环境
if ! command -v java &> /dev/null; then
    echo "❌ Java 未安装"
    exit 1
fi

# 检查服务器是否运行
if ! nc -z 127.0.0.1 3307 2>/dev/null; then
    echo "⚠️  MySQL 服务未在 3307 端口运行"
    echo "请先启动服务: ./target/release/calm mysql --port 3307"
    exit 1
fi

echo "✅ MySQL 服务正在运行"

# 检查 MySQL Connector JAR
MYSQL_JAR=$(find . -name "mysql-connector-*.jar" 2>/dev/null | head -1)
if [ -z "$MYSQL_JAR" ]; then
    echo "⚠️  未找到 mysql-connector-java.jar"
    echo "下载地址: https://dev.mysql.com/downloads/connector/j/"
    CLASSPATH="."
else
    echo "✅ 找到 MySQL Connector: $MYSQL_JAR"
    CLASSPATH=".:$MYSQL_JAR"
fi

# 编译测试程序
echo ""
echo "编译测试程序..."
javac test_jdbc_connection.java

if [ $? -ne 0 ]; then
    echo "❌ 编译失败"
    exit 1
fi

echo "✅ 编译成功"
echo ""
echo "========================================"
echo "运行连接测试..."
echo "========================================"

# 运行测试
java -cp "$CLASSPATH" test_jdbc_connection

echo ""
echo "========================================"
echo "测试完成"
echo "========================================"
