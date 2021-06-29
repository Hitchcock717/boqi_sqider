# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @File : boqi_spider.py
# @Author: Hitchcock
# @Date:    : 2021/6/26
# @Desc :
"""
波奇宠物爬虫
"""
import os
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
import time
import math
import json
import logging
import requests
from retrying import retry
from bs4 import BeautifulSoup
from multiprocessing.dummy import Pool as ThreadPool

pics_folder = os.getcwd() + '/pics/'
info_folder = os.getcwd() + '/infos/'
# categories = ['dog', 'cat', 'smallpet', 'aquarium', 'reptile']
categories = ['plant']

class BoqiSpider(object):

    def __init__(self):
        self.pet_url = 'http://www.boqii.com/pet-all/'
        self.know_url = 'https://baike.baidu.com/item/'

        self.pool = ThreadPool(4)
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/91.0.4472.77 Safari/537.36'
        }
        # /*** Logging Config ***/ #
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(level=logging.INFO)
        handler = logging.FileHandler("LOG.log")
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(fmt=formatter)
        self.logger.addHandler(handler)

    @retry(stop_max_attempt_number=10)
    def request_html(self, url):
        r = requests.get(url, headers=self.headers, timeout=10)
        if r.status_code == 200:
            self.logger.info('状态响应【%s】---> 爬取链接成功：%s' % (r.status_code, url))
            print('状态响应【%s】---> 爬取链接成功：%s' % (r.status_code, url))
            return r.text
        elif r.status_code == 429:
            time.sleep(int(r.headers["Retry-After"]))
        else:
            self.logger.error('状态响应【%s】---> 爬取链接失败：%s' % (r.status_code, url))
            print('状态响应【%s】---> 爬取链接失败：%s' % (r.status_code, url))
            return None

    def start_requests(self):
        self.logger.info('===================== 爬虫开始 =====================')
        if not os.path.exists(info_folder):
            os.makedirs(info_folder)

        for category in categories:
            category_pet_pics = []
            category_pet_names = []
            category_pet_names_success = []
            category_pet_pics_success = []
            category_pet_infos = []
            category_pets = {}

            category_pic_folder = pics_folder + category + '/'
            if not os.path.exists(category_pic_folder):
                os.makedirs(category_pic_folder)
            category_info_json = info_folder + category + '.json'
            init_url = self.pet_url + category + '/'
            content = self.request_html(init_url)
            if not content:
                self.logger.error('爬取【%s】类目失败！' % category)
                continue
            soup = BeautifulSoup(content, 'lxml')
            category_pet_number = soup.find(class_='sear_tit_l left').find('span').get_text()
            category_pet_page = math.ceil(int(category_pet_number) / 30)
            print('【%s】类目共计%s页' % (category, category_pet_page))
            pets = soup.find(class_='hot_pet_cont')
            category_pet_pics += [dl.find('img').get('src') for dl in pets.findAll('dl')]
            category_pet_names += [dl.find('dd').get_text() for dl in pets.findAll('dl')]
            next_urls = [(init_url + '?p=' + str(page)) for page in range(2, category_pet_page + 1)]
            for next_url in next_urls:
                next_content = self.request_html(next_url)
                if not next_content:
                    self.logger.error('爬取下一页链接失败：%s' % next_url)
                    continue
                soup_n = BeautifulSoup(next_content, 'lxml')
                next_pets = soup_n.find(class_='hot_pet_cont')
                category_pet_pics += [dl.find('img').get('src') for dl in next_pets.findAll('dl')]
                category_pet_names += [dl.find('dd').get_text() for dl in next_pets.findAll('dl')]

            for i in range(len(category_pet_names)):
                item_url = self.know_url + category_pet_names[i]
                content = self.request_html(item_url)
                if not content:
                    self.logger.error('爬取【%s】词条失败！' % category_pet_names[i])
                    print('爬取【%s】词条失败！' % category_pet_names[i])
                    continue
                soup1 = BeautifulSoup(content, 'lxml')
                if soup1.find(class_='lemma-summary'):
                    category_pet_infos.append(soup1.find(class_='lemma-summary').get_text().strip('\r\n'))
                    category_pet_pics_success.append(category_pet_pics[i])
                    category_pet_names_success.append(category_pet_names[i])
                    print('成功爬取【%s】的词条！' % category_pet_names[i])
                else:
                    if soup1.find(class_='para'):
                        c_url = 'https://baike.baidu.com' + soup1.findAll(class_='para')[0].find('a').get('href')
                        soup2 = BeautifulSoup(self.request_html(c_url), 'lxml')
                        if not soup2.find(class_='lemma-summary'):
                            print('错误链接！！！ ' + c_url)
                        else:
                            category_pet_infos.append(soup2.find(class_='lemma-summary').get_text().strip('\r\n'))
                            category_pet_pics_success.append(category_pet_pics[i])
                            category_pet_names_success.append(category_pet_names[i])
                            print('成功爬取【%s】的词条！' % category_pet_names[i])

            print('【%s】类目爬取完成，共计%s项素材，成功爬取%s项素材，%s项词条，%s项图片' % (category,
                                                                        len(category_pet_names),
                                                                        len(category_pet_names_success),
                                                                        len(category_pet_infos),
                                                                        len(category_pet_pics_success)))

            index = 0
            for (url, name, info) in zip(category_pet_pics_success, category_pet_names_success, category_pet_infos):
                pet_elem = dict()
                pet_elem['url'] = url
                pet_elem['value'] = info
                pet_elem['index'] = index
                category_pets[name] = pet_elem
                index += 1

            with open(category_info_json, 'w+') as f:
                json_content = json.dumps(category_pets, ensure_ascii=False, indent=4)
                f.write(json_content)
                print('【%s】类目存储完成' % category)

            '''
            # 下载图片
            for j in range(len(category_pet_pics_success)):
                pic_name = category_pic_folder + '0' + str(j+1) + '_' + category_pet_names_success[j] + '.png'
                if not os.path.exists(pic_name) or not os.path.getsize(pic_name):
                    self.download_pics(pic_name, category_pet_pics_success[j], j)

            # 重新下载空图片
            for path, dir_list, file_list in os.walk(category_pic_folder):
                for i in range(len(file_list)):
                    pic_download = os.path.join(path, file_list[i])
                    if not os.path.getsize(pic_download):
                        path, filename = os.path.split(pic_download)
                        print(filename)
                        pet_name = os.path.splitext(filename.split('_')[1])[0]
                        j = int(filename.split('_')[0]) - 1
                        pic_download_url = category_pet_pics[category_pet_names.index(pet_name)]
                        print(pic_download_url)
                        self.download_pics(pic_download, pic_download_url, j)
            '''

        self.logger.info('===================== 爬虫结束 =====================')

    def download_pics(self, pic, pic_url, index):
        with open(pic, 'wb') as f:
            print('正在下载图片：' + pic_url)
            f.write(requests.get(pic_url).content)
            f.flush()
        f.close()
        print('第%d张图片下载完成' % (index + 1))
        time.sleep(1)


if __name__ == '__main__':
    BoqiSpider().start_requests()
