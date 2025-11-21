#!/usr/bin/env python3
"""
启动 Calm 测试服务器

使用方法：
    python3 start_server.py [--data-dir DIR] [--mysql-port PORT] [--es-port PORT]
"""

import os
import sys
import subprocess
import signal
import argparse
import time

# 颜色
class Colors:
    BLUE = '\033[0;34m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    RED = '\033[0;31m'
    NC = '\033[0m'

def print_colored(color, text):
    print(f"{color}{text}{Colors.NC}")

def build_project():
    """编译项目"""
    print_colored(Colors.YELLOW, "📦 Building project...")
    
    try:
        result = subprocess.run(
            ['cargo', 'build', '--release'],
            cwd='..',
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            print_colored(Colors.RED, f"❌ Build failed:\n{result.stderr}")
            return False
        
        print_colored(Colors.GREEN, "✓ Build complete\n")
        return True
        
    except Exception as e:
        print_colored(Colors.RED, f"❌ Build error: {e}")
        return False

def check_binary_exists():
    """检查二进制文件是否存在"""
    binary_path = '../target/release/calm'
    return os.path.exists(binary_path)

def start_server(data_dir, mysql_port, es_port, graphql_port, log_level='info'):
    """启动服务器"""
    print_colored(Colors.BLUE, "=== Starting Calm Test Server ===\n")
    
    # 每次都编译二进制
    if not build_project():
        return False
    
    # 清理旧数据（可选）
    if os.path.exists(data_dir):
        print_colored(Colors.YELLOW, f"Cleaning old test data: {data_dir}")
        import shutil
        shutil.rmtree(data_dir)
    
    print_colored(Colors.BLUE, "🚀 Starting server...")
    print(f"  Data directory: {data_dir}")
    print(f"  MySQL port: {mysql_port}")
    print(f"  GraphQL port: {graphql_port}")
    print(f"  Elasticsearch port: {es_port}")
    print(f"  Log level: {log_level}")
    print()
    
    # 启动服务器进程
    cmd = [
        '../target/release/calm',
        '--data-dir', data_dir,
        '--mysql-port', str(mysql_port),
        '--graphql-port', str(graphql_port),
        '--es-port', str(es_port),
        '--log-level', log_level
    ]
    
    try:
        process = subprocess.Popen(cmd)
        
        print_colored(Colors.GREEN, f"✓ Server started (PID: {process.pid})")
        print_colored(Colors.BLUE, "\n📊 Server endpoints:")
        print(f"  MySQL: mysql -h 127.0.0.1 -P {mysql_port} -u root")
        print(f"  GraphQL: http://127.0.0.1:{graphql_port}/graphql")
        print(f"  GraphQL Playground: http://127.0.0.1:{graphql_port}/playground")
        print(f"  Elasticsearch: http://127.0.0.1:{es_port}")
        print()
        print_colored(Colors.YELLOW, "Press Ctrl+C to stop the server\n")
        
        # 等待服务器启动
        time.sleep(2)
        
        # 检查进程是否还在运行
        if process.poll() is not None:
            print_colored(Colors.RED, "❌ Server process exited unexpectedly")
            return False
        
        # 等待进程结束或 Ctrl+C
        def signal_handler(sig, frame):
            print_colored(Colors.YELLOW, "\n\n🛑 Shutting down server...")
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                print_colored(Colors.YELLOW, "Force killing server...")
                process.kill()
            print_colored(Colors.GREEN, "✓ Server stopped")
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # 等待进程
        process.wait()
        
        return True
        
    except Exception as e:
        print_colored(Colors.RED, f"❌ Failed to start server: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Start Calm test server')
    parser.add_argument('--data-dir', default='./test_data', help='Data directory')
    parser.add_argument('--mysql-port', type=int, default=3306, help='MySQL port')
    parser.add_argument('--graphql-port', type=int, default=8000, help='GraphQL port')
    parser.add_argument('--es-port', type=int, default=9200, help='Elasticsearch port')
    parser.add_argument('--log-level', default='info', choices=['trace', 'debug', 'info', 'warn', 'error'], help='Log level')
    
    args = parser.parse_args()
    
    success = start_server(args.data_dir, args.mysql_port, args.es_port, args.graphql_port, args.log_level)
    
    return 0 if success else 1

if __name__ == '__main__':
    sys.exit(main())
