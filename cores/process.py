import base64
import datetime
import random
from multiprocessing import Process

import requests

from cores import utility


class DownloadProcessError(Exception):

    def __init__(self, img_url, msg, err_code):
        self.err_img_url = img_url
        self.msg = msg
        self.err_code = err_code

    def __map_msg(self):
        msgs = {
            0: "Download Process ",
            1: "Process ",
            2: "Insert "
        }
        return msgs[self.err_code] + "Error Occur(" + self.err_img_url['url'] + "): " + self.msg

    def __str__(self):
        return self.__map_msg()



class DownloadProcess(Process):

    def __init__(self, url_queue, db_pool, proxy_ip_server):
        super(DownloadProcess, self).__init__()
        self.url_queue = url_queue
        self.logger = utility.init_console_log(str(datetime.date.today()))
        self.err_logger = utility.init_file_log(str(datetime.date.today()) + ".err")
        self.user_agents = self.__load_file('user_agents')
        self.server = proxy_ip_server
        self.db_pool = db_pool


    def __load_file(self, file_path, shuffle=True):
        contents = []
        with open(file_path, 'r') as f:
            for line in f.readlines():
                if line:
                    contents.append(line.strip()[1:-1])
        if shuffle:
            random.shuffle(contents)
        return contents


    def download_error_handler(self, err):
        if err.err_code == 0 and err.err_img_url['err'] < 2:
            err.err_img_url['err'] += 1
            self.url_queue.put(err.err_img_url)
            self.err_logger.warning(str(err))
        else:
            self.err_logger.error(str(err))



    def download_image(self, url):
        self.logger.info("Downloading Image (" + url['url'] + ") ")
        random_agent = random.choice(self.user_agents)
        random_proxy = self.server.get_proxy_ip()
        hdr = {'User-Agent': random_agent,
               'Accept-Language': 'en-US,en;q=0.5',
               'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
               'Accept-encoding': 'gzip, deflate, sdch, br'
               }
        try:
            if random_proxy and isinstance(random_proxy,dict):
                page = requests.get(url['url'], headers=hdr, verify=False, timeout=20,
                                    proxies={'http': random_proxy, 'https': random_proxy})
            else:
                page = requests.get(url['url'], headers=hdr, verify=False, timeout=20)
            img = page.content
        except Exception as err:
            raise DownloadProcessError(url,str(err),0)
        return img

    def process_image(self, img, img_url):
        self.logger.info("Processing Image (" + str(img_url) + ") ")
        try:
            # nparr = np.fromstring(img, np.uint8)
            # img_arry = cv2.imdecode(nparr, cv2.IMREAD_UNCHANGED)
            # nimage = image.normalize_img(img_arry)
            # img_en = cv2.imencode('.jpg', nimage)
            img64 = base64.b64encode(img)
        except Exception as err:
            raise DownloadProcessError(img_url,str(err),1)
        return img64

    def insert_to_db(self, img_url,img64):
        self.logger.info("Inserting Image (" + img_url + ") ")
        sql = "insert into image_warehouse (image_url, image_64) values(%s,%s)"
        db = self.db_pool.get()
        try:
            db.cursor.execute(sql,(img_url['url'], img64))
            db.cnx.commit()
        except Exception as err:
            raise DownloadProcessError(img_url, str(err),2)
        finally:
            self.db_pool.put(db)


    def run(self):
        while not self.url_queue.empty():
            img_url = self.url_queue.get()
            try:
                img = self.download_image(img_url)
                img64 = self.process_image(img,img_url)
                self.insert_to_db(img_url, img64)
            except DownloadProcessError as err:
                self.download_error_handler(err)
            except Exception as err:
                self.logger.err("Unexpect Error Occur at :" + str(img_url['url']) + " " + str(err))