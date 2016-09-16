from multiprocessing import Queue

import settings

from cores import database, process, server


def get_all_img_urls():
    sql = "select image_id, image_url from image_list;"
    db = database.Instance(settings.DATABASE_INFO_CONFIG)
    img_urls = db.execute_select_sql(sql)
    img_queue = Queue()
    for img in img_urls:
        img_queue.put({"id":img[0],"url":img[1],"err":0} )
    return img_queue

def init_db_pool():
    db_pool = Queue()
    for _ in range(0, settings.NUM_DB_CONNECTION):
        db_pool.put(database.Instance(settings.DATABASE_INFO_CONFIG))
    return db_pool



def main():
    all_img_urls = get_all_img_urls()
    db_pool = init_db_pool()
    proxy_ip_server = server.ProxyServer(**settings.SERVER_INFO_CONFIG)
    processes = [process.DownloadProcess(all_img_urls, db_pool, proxy_ip_server)
                 for _ in range(0, settings.NUM_CONCURRENCY)]
    for p in processes:
        p.start()
    for p in processes:
        p.join()


def test_main():
    all_img_urls = Queue()
    all_img_urls.put({"id":"1234","url":"http://images10.newegg.com/ProductImage/A64R_1_20160219060836384.jpg","err":0})
    p = process.DownloadProcess(all_img_urls, settings.SERVER_INFO_CONFIG, settings.DATABASE_INFO_CONFIG)
    p.start()
    p.join()



if __name__=="__main__":
    main()