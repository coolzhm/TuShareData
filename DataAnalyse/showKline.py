'''
从数据库中读取相关的股票并绘制K线图，此处使用的是pyecharts中自带的K线图
'''
import pymysql
import config
from pyecharts import Kline, Candlestick
import pandas as pd


# 从数据库获取某个股票从当前时间算起一年的数据
def get_data_from_db(code, start, end):
    '''
    数据库中读取指定时间区间的股票代码code的数据
    :param code: 股票代码
    :param start: 开始时间(包含)
    :param end: 结束时间(包含)
    :return:
    '''
    connection = pymysql.connect(**config.mysql_info)
    return pd.read_sql('''
                    SELECT
                        t.date,
                        t.`open`,                        
                        t.`close`,
                        t.low,
                        t.high
                    FROM
                        stock_history t
                    WHERE
                        t.`code` = '{0}'
                    AND t.date >= str_to_date('{1}','%Y-%m-%d')
                    AND t.date <= str_to_date('{2}','%Y-%m-%d')
                    order by t.date asc
    '''.format(code, start, end), con=connection, index_col='date')


def view(code, start_, end_):
    '''
    使用pyecharts展示数据
    :param code: 股票代码
    :param start_: 开始时间（包含）格式为'2018-01-01'
    :param end_: 结束时间（包含）格式为'2018-01-01'
    :return:
    '''
    # 从数据库获取数据
    th = get_data_from_db(code, start_, end_)

    '''
    官网网址：http://pyecharts.org/#/zh-cn/charts?id=klinecandlestick%ef%bc%88k%e7%ba%bf%e5%9b%be%ef%bc%89
    Kline.add() 方法签名
            add(name, x_axis, y_axis, **kwargs)
    name -> str
    图例名称
    x_axis -> list
    x 坐标轴数据
    y_axis -> [list], 包含列表的列表
    y 坐标轴数据。数据中，每一行是一个『数据项』，每一列属于一个『维度』。
     数据项具体为 [open, close, lowest, highest] （即：[开盘值, 收盘值, 最低值, 最高值]）
    '''
    # 此处存放的是K线数组
    v1 = th.values.tolist()
    # 存放X轴横坐标数组，此处选用的是日期
    v2 = [str(x.strftime('%Y-%m-%d')) for x in th.index]
    kline = Kline("{0} K线图".format(code))
    '''
    is_datazoom_show:是否显示放大缩小
    mark_point:标记最大最小值
    '''
    kline.add("日K", v2, v1, is_datazoom_show=True, mark_point=["min", "max"])
    # 打印配置信息
    kline.show_config()
    try:
        kline.render('股票[{0}]K线图.html'.format(code))
    except Exception as e:
        print('生成图形出错啦[{0}]'.format(e))


if __name__ == '__main__':
    view(code='600360', start_='2017-04-01', end_='2018-04-16')
