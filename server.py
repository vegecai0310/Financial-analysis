from flask import Flask, render_template, send_from_directory, send_file
import socket
import os
import logging

# 获取当前文件所在目录的绝对路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')

app = Flask(__name__)

def find_available_port(start_port=5000, max_tries=10):
    """查找可用端口"""
    for port in range(start_port, start_port + max_tries):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.bind(('localhost', port))
            sock.close()
            return port
        except OSError:
            sock.close()
            continue
    raise RuntimeError(f"在端口范围 {start_port}-{start_port + max_tries - 1} 内没有找到可用端口")

@app.route('/')
def index():
    return send_file('index.html')  # 直接从根目录发送文件

@app.route('/data/<path:filename>')
def serve_data(filename):
    # 确保data目录存在
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    return send_from_directory(DATA_DIR, filename)

if __name__ == "__main__":
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)  # 设置日志级别为 ERROR，减少输出
    
    start_port = 5002
    max_tries = 10
    port = find_available_port(start_port, max_tries)
    
    print(f"服务器启动中...")
    print(f"请访问：http://localhost:{port}")
    try:
        app.run(port=port, debug=False)  # 关闭 debug 模式
        print("服务器已启动")
    except OSError as e:
        if "Address already in use" in str(e):
            print(f"端口 {port} 已被占用，正在尝试其他端口...")
            port = find_available_port(port + 1, max_tries)
            print(f"找到可用端口：{port}")
            print(f"请访问：http://localhost:{port}")
            app.run(port=port, debug=False)
            print("服务器已启动")
        else:
            raise e 