#-coding:utf-8
from multiprocessing.pool import Pool

import requests
from bs4 import BeautifulSoup
import pymongo
import re
                                                    #安装这个pymongo后，调用这个方法，
client = pymongo.MongoClient('192.168.56.11', 27017)  #输入数据库所在的ip，端口，这里不用实现创建数据库
gaokao = client['gaokao']                           #这里相当于创建了gaokao这个数据库
provice_href = gaokao['provice_href']               #创建这个provice_href的表赋变量给他
score_detail = gaokao['score_detail']
# db.collection.find(query, {title: 1, by: 0})
res=score_detail.find({"provice" : '陕西'})          #查找这个表中表列名为provice,值为陕西的
# print(res,dir(res))
# for i in res:
#     print(i)
# 获取省份及链接
pro_link = []
header = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:58.0) Gecko/20100101 Firefox/58.0',
    'Connection': 'keep - alive'}
def get_provice(url):
    web_data = requests.get(url, headers=header)
    soup = BeautifulSoup(web_data.content, 'lxml')
    provice_link = soup.select('.area_box > a')
    for link in provice_link:
        href = link['href']
        provice = link.select('span')[0].text
        data = {
            'href': href,
            'provice': provice
        }
        # provice_in_mongodb = provice_href.find({"provice": provice})
        provice_in_mongodb = provice_href.find({"$and":[{'provice': provice},{'href':href}]})

        if provice_in_mongodb.count():  # 判断查找这个条件的出来的数据是否为空，注意这里是用.count()
            print('已存在，更新url')
            provice_href.update({'provice':provice},data) # 这里是更新语句，前面是条件，后面是更新的内容，都是字典格式
        else:
            provice_href.insert_one(data)
        pro_link.append(href)
    print('OK')


# 获取分数线
def get_score(url):
    web_data = requests.get(url, headers=header)
    soup = BeautifulSoup(web_data.content, 'lxml')
    # 获取省份信息
    provice = soup.select('.col-nav span')[0].text[0:-5]
    # 获取文理科
    categories = soup.select('h3.ft14')
    category_list = []
    for item in categories:
        category_list.append(item.text.strip().replace(' ', ''))
    # 获取分数
    tables = soup.select('h3 ~ table')
    try:
        for index, table in enumerate(tables):
            tr = table.find_all('tr', attrs={'class': re.compile('^c_\S*')})
            for j in tr:
                td = j.select('td')
                score_list = []
                score_line = ''
                for k in td:
                    # 获取每年的分数
                    if 'class' not in k.attrs:
                        score = k.text.strip()
                        score_list.append(score)

                    # 获取分数线类别
                    elif 'class' in k.attrs:
                        score_line = k.text.strip()

                    score_data = {
                        'provice': provice.strip(),#省份
                        'category': category_list[index],#文理科分类
                        'score_line': score_line,#分数线类别
                        'score_list': score_list#分数列表
                    }
                # 查询条件有多个的时候，就全部卸载一个字典里面，类似于下面这种，and条件类型
                score_detail_in_mongodb = score_detail.find({'provice': provice.strip(),'category': \
                                                            category_list[index],'score_line': score_line})

                # print(score_detail_in_mongodb.count())
                if score_detail_in_mongodb.count(): #下面是更新语句，最后一个加上需要更新后的数据，注意是字典格式的
                    print('分数线已存在，更新')
                    score_detail.update({'provice': provice.strip(),'category': \
                                                            category_list[index],'score_line': score_line},score_data)
                else:
                    score_detail.insert_one(score_data)
            print("detail insert ok")
    except Exception as e:
        print('insert error',e)
        import sys
        print("file: [ %s ], line: [ %s ]"
                    % (__file__, sys._getframe().f_lineno))



if __name__ == '__main__':

    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:58.0) Gecko/20100101 Firefox/58.0',
        'Connection': 'keep - alive'}
    url = 'http://www.gaokao.com/guangdong/fsx/'
    get_provice(url)
    # pool = Pool()
    # pool.map(get_score, [i for i in pro_link])#使用多线程
