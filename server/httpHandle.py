 #-*- coding:utf-8 -*-
import sys, os, subprocess
from urllib.parse import quote,unquote
#-------------------------------------------------------------------------------

class ServerException(Exception):
    '''服务器内部错误'''
    pass

#-------------------------------------------------------------------------------

class base_case(object):
    '''条件处理基类'''

    suffix_type = ["css", "js"]
    image_suffix = ["jpg", "icn", "png"]
    video_suffix = ["mp4"]

    def getRequestsSuffix(self, handler):
        return unquote(handler.path).split(".")[-1]

    def handle_file(self, handler, full_path):
        try:
            with open(full_path, 'rb') as reader:
                content = reader.read()
            suffix = self.getRequestsSuffix(handler)
            if suffix in self.suffix_type:
                eval("handler.send_" + suffix)(content)
            elif suffix in self.image_suffix:
                eval("handler.send_" + "image")(content)
            elif suffix in self.video_suffix:
                eval("handler.send_" + "video")(content)
            else:
                handler.send_html(content)

        except IOError as msg:
            msg = "'{0}' cannot be read: {1}".format(full_path, msg)
            handler.handle_error(msg)

    def index_path(self, handler):
        return os.path.join(handler.full_path, 'html/home.html')

    def test(self, handler):
        assert False, 'Not implemented.'

    def act(self, handler):
        assert False, 'Not implemented.'


# -------------------------------------------------------------------------------

class case_no_file(base_case):
    '''文件或目录不存在'''

    def test(self, handler):
        return not os.path.exists(handler.full_path)

    def act(self, handler):
        handler.full_path = handler.base_path + "/html" + unquote(handler.path)
        if self.test(handler):
            raise ServerException("'{0}' not found".format(handler.path))


# -------------------------------------------------------------------------------

class case_cgi_file(base_case):
    '''可执行脚本'''

    def run_cgi(self, handler):
        data = subprocess.check_output(["python3", handler.full_path], shell=False)
        handler.send_content(data)

    def test(self, handler):
        return os.path.isfile(handler.full_path) and \
               handler.full_path.endswith('.py')

    def act(self, handler):
        self.run_cgi(handler)


# -------------------------------------------------------------------------------

class case_existing_file(base_case):
    '''文件存在的情况'''

    def test(self, handler):
        return os.path.isfile(handler.full_path)

    def act(self, handler):
        self.handle_file(handler, handler.full_path)


# -------------------------------------------------------------------------------

class case_directory_index_file(base_case):
    '''在根路径下返回主页文件'''

    def test(self, handler):
        return os.path.isdir(handler.full_path) and \
               os.path.isfile(self.index_path(handler))

    def act(self, handler):
        self.handle_file(handler, self.index_path(handler))


# -------------------------------------------------------------------------------

class case_always_fail(base_case):
    '''默认处理'''

    def test(self, handler):
        return True

    def act(self, handler):
        raise ServerException("Unknown object '{0}'".format(handler.path))
