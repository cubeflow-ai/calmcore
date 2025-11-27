#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🔍 查找 Druid JAR 文件..."
DRUID_JAR=$(find ~/.m2/repository/com/alibaba/druid -name "druid-*.jar" 2>/dev/null | head -1)
MYSQL_JAR=$(find ~/.m2/repository/mysql/mysql-connector-java -name "mysql-connector-java-*.jar" 2>/dev/null | head -1)

if [ -z "$DRUID_JAR" ]; then
    echo "❌ 找不到 Druid JAR 文件"
    echo "请先安装: mvn dependency:get -Dartifact=com.alibaba:druid:1.2.16"
    exit 1
fi

if [ -z "$MYSQL_JAR" ]; then
    echo "❌ 找不到 MySQL Connector JAR 文件"
    echo "请先安装: mvn dependency:get -Dartifact=mysql:mysql-connector-java:8.0.33"
    exit 1
fi

echo "✅ Druid JAR: $DRUID_JAR"
echo "✅ MySQL JAR: $MYSQL_JAR"

echo ""
echo "🔨 编译 TestDruidConnection.java..."
javac -cp "$DRUID_JAR:$MYSQL_JAR" TestDruidConnection.java

if [ $? -ne 0 ]; then
    echo "❌ 编译失败"
    exit 1
fi

echo "✅ 编译成功"
echo ""
echo "🚀 运行测试..."
echo "================================"
java -cp ".:$DRUID_JAR:$MYSQL_JAR" TestDruidConnection
