import urllib.request
from urllib.parse import urljoin
import json

class BaseServer():
    def __init__(self, server_address, server_user,server_secret):
        self.server_address = server_address
        self.server_user = server_user
        self.server_secret = server_secret


    def send_json_data(self, dict_data,server_sub):
        server = urljoin(self.server_address,server_sub)
        if server[-1] != "/":
            server += "/"
        request = urllib.request.Request(server)
        request.add_header('Content-Type', 'application/json')
        dict_data.update({"user":self.server_user,"secret":self.server_secret})
        jsondata = json.dumps(dict_data)
        jsondatabytes = jsondata.encode('utf-8')
        request.add_header('Content-Length', len(jsondatabytes))
        response = urllib.request.urlopen(request, jsondatabytes)
        return response


class ProxyServer(BaseServer):

    def __init__(self, server_address, server_user,server_secret):
        super(ProxyServer,self).__init__(server_address,server_user,server_secret)
        self.used_proxyid=[]


    def get_proxy_ip(self):
        proxy = ''
        try:
            jdata = {'id':self.used_proxyid}
            response = self.send_json_data(jdata,"proxyip/get_proxyip")
            res = response.read()
            if res:
                proxy = json.loads(res.decode('utf8'))
                self.used_proxyid.append(proxy['id'])
        except Exception as err:
            print(str(err))
        return proxy

    def update_failure(self):
        try:
            jdata = {'id': self.used_proxyid[-1]}
            response = self.send_json_data(jdata,"proxyip/update/")
            if response.getcode() == 200:
                return True
            else:
                return False
        except Exception as err:
            print(str(err))

    def clean(self):
        self.used_proxyid = []