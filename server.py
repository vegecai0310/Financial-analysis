import http.server
import socketserver
import os

# 设置端口号
PORT = 8000

# 获取当前文件所在目录的绝对路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 切换到项目根目录
os.chdir(BASE_DIR)

class MyHttpRequestHandler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        # 添加 CORS 头，允许跨域请求
        self.send_header('Access-Control-Allow-Origin', '*')
        super().end_headers()

Handler = MyHttpRequestHandler

with socketserver.TCPServer(("", PORT), Handler) as httpd:
    print(f"服务器启动在端口 {PORT}")
    print(f"请访问: http://localhost:{PORT}")
    httpd.serve_forever() 