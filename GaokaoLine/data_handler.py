import pymongo
import charts
client = pymongo.MongoClient('10.83.36.87', 27017)
gaokao = client['gaokao']
score_detail = gaokao['score_detail']

# 筛选分数线、省份、文理科
def get_score(line,pro,cate):
    score_list=[]
    for i in score_detail.find({"$and":[{"score_line":line},{"provice":pro},{'category': cate}]}):
        score_list = i['score_list']
        print('----',score_list)
        if '-' in score_list:
            score_list.remove('-')#去掉没有数据的栏目
        score_list = list(map(int, score_list))
        score_list.reverse()
        return score_list


###########################################################


# 获取文理科分数
line = '一本'
pro = '山东'
cate_wen = '文科'
cate_li = '理科'
wen=[]
li = []
wen=get_score(line,pro,cate_wen)#文科
li=get_score(line,pro,cate_li)#理科

# 定义年份
year = [2018,2017,2016,2015,2014,2013,2012,2011,2010,2009]
year.reverse()

###########################################################

series = [
    {
    'name': '文 科',
    'data': wen,
    'type': 'line'
}, {
    'name': '理科',
    'data': li,
    'type': 'line',
    'color':'#ff0066'
}
         ]
options = {
    'chart'   : {'zoomType':'xy'},
    'title'   : {'text': '{}省{}分数线'.format(pro,line)},
    'subtitle': {'text': 'Source: gaokao.com'},
    'xAxis'   : {'categories': year},
    'yAxis'   : {'title': {'text': 'score'}}
    }
try:
    charts.plot(series, options=options,show='inline')
    # charts.plot(series, options=options,show='tab')
except Exception as e:
    print('生成图片失败: ',e)