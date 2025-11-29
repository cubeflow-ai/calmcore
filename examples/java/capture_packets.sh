#!/bin/bash

# 捕获 MySQL 协议包
echo "📦 捕获 MySQL 服务器的响应..."
sudo tcpdump -i lo0 -s 0 -w /tmp/mysql_real.pcap "tcp port 3306" &
PID_REAL=$!
sleep 1

# 连接真实 MySQL
java -cp ".:$HOME/.m2/repository/mysql/mysql-connector-java/8.0.30/mysql-connector-java-8.0.30.jar" TestMySQLComparison > /dev/null 2>&1

sleep 1
sudo kill $PID_REAL
echo "✅ MySQL 捕获完成: /tmp/mysql_real.pcap"

echo ""
echo "📦 捕获 Calm 服务器的响应..."
sudo tcpdump -i lo0 -s 0 -w /tmp/mysql_calm.pcap "tcp port 3307" &
PID_CALM=$!
sleep 1

# 连接 Calm
java -cp ".:$HOME/.m2/repository/mysql/mysql-connector-java/8.0.30/mysql-connector-java-8.0.30.jar" TestMySQLComparison > /dev/null 2>&1

sleep 1
sudo kill $PID_CALM
echo "✅ Calm 捕获完成: /tmp/mysql_calm.pcap"

echo ""
echo "📊 使用 Wireshark 打开这两个文件进行对比:"
echo "   /tmp/mysql_real.pcap"
echo "   /tmp/mysql_calm.pcap"
