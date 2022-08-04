#-*- coding:utf-8 -*-
import sys, os, subprocess
from http.server import BaseHTTPRequestHandler,HTTPServer
from httpHandle import *
from urllib.parse import quote,unquote

import json
#-------------------------------------------------------------------------------

class RequestHandler(BaseHTTPRequestHandler):
    '''
    请求路径合法则返回相应处理
    否则返回错误页面
    '''

    Cases = [
             case_no_file(),
             case_cgi_file(),
             case_existing_file(),
             case_directory_index_file(),
             case_always_fail()]

    # 错误页面模板
    Error_Page = """\
        <html>
        <body>
        <h1>Error accessing {path}</h1>
        <p>{msg}</p>
        </body>
        </html>
        """
    type_dict = {
        "html": "text/html",
        "css": "text/css",
        "js": "application/javascript",
        "jpg": "image/jpeg",
        "png": "image/png",
        "video": "video/mp4",
    }
    base_path = "/".join(os.path.dirname(__file__).split("\\")[:-1])  # 站点

    def do_GET(self):
        try:

            # 得到完整的请求路径
            self.full_path = self.base_path + unquote(self.path)
            print(self.full_path)
            print(self.headers)
            # self.full_path = self.Cases[0].filePath(self)

            # 遍历所有的情况并处理
            c = 1
            for case in self.Cases:
                if c == 4:
                    self.full_path = self.base_path
                if case.test(self):
                    case.act(self)
                    if c != 1:
                        break
                c += 1

        # 处理异常
        except Exception as msg:
            print(msg)
            self.handle_error(msg)

    def do_POST(self):
        print("aaa:", unquote(self.path))
        req_datas = self.rfile.read(int(self.headers['content-length']))  # 重点在此步!
        with open(self.base_path + self.path, "wb") as f:
            f.write(req_datas)
        print("上传成功！")
        # print(req_datas)
        # print(object1)
        # print(object1[0], object1[1])
        # print(type(object1))      # , type(object1[0]), type(object1[1])
        # print(json.JSONDecoder.decode(object1[0], object1[1]))

    def handle_error(self, msg):
        content = self.Error_Page.format(path=self.path, msg=msg)
        self.send_content(content.encode("utf-8"), 404)

    def send_html(self, content, status=200):
        """
        send html
        :param content:
        :param status:
        :param type:
        :return:
        """
        self.send_content(content, types=self.type_dict["html"])

    def send_css(self, content, status=200):
        """
        send html
        :param content:
        :param status:
        :param type:
        :return:
        """
        self.send_content(content, types=self.type_dict["css"])

    def send_js(self, content, status=200):
        """
        send html
        :param content:
        :param status:
        :param type:
        :return:
        """
        self.send_content(content, types=self.type_dict["js"])

    def send_image(self, content, status=200):
        """
        send html
        :param content:
        :param status:
        :param type:
        :return:
        """
        self.send_content(content, types=self.type_dict[self.Cases[0].getRequestsSuffix(self)])

    def send_video(self, content, status=200):
        """
        send html
        :param content:
        :param status:
        :param type:
        :return:
        """
        self.send_response(status)
        self.send_header("Content-type", self.type_dict["video"])
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("content-disposition", "attachment;filename=" + quote(os.path.basename(self.full_path)))
        self.end_headers()
        self.wfile.write(content)

    # def send_html(self, content, status=200):
    #     """
    #     send html
    #     :param content:
    #     :param status:
    #     :param type:
    #     :return:
    #     """
    #     self.send_content(content, type=self.type_dict["html"])


    # 发送数据到客户端
    def send_content(self, content, status=200, types=None):
        self.send_response(status)
        self.send_header("Content-type", types)
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

#-------------------------------------------------------------------------------

if __name__ == '__main__':
    serverAddress = ('', 8080)
    server = HTTPServer(serverAddress, RequestHandler)
    print("服务器启动")
    server.serve_forever()