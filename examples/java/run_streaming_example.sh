#!/bin/bash

# 编译和运行 Java 流式读取示例

# 下载 MySQL JDBC 驱动（如果不存在）
JDBC_JAR="mysql-connector-j-8.2.0.jar"
if [ ! -f "$JDBC_JAR" ]; then
    echo "📦 下载 MySQL JDBC 驱动..."
    curl -O https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.2.0/mysql-connector-j-8.2.0.jar
fi

# 编译
echo "🔨 编译 StreamingReadExample.java..."
javac -cp .:$JDBC_JAR StreamingReadExample.java

if [ $? -ne 0 ]; then
    echo "❌ 编译失败"
    exit 1
fi

# 运行
echo ""
echo "🚀 运行流式读取示例..."
echo "======================================"
java -cp .:$JDBC_JAR StreamingReadExample

echo ""
echo "✅ 完成"
