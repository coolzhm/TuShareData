'''
计算股票的波动率
这里衡量股票波动指标我们定义为 最高价/最低价，比率越大我们从中获利的可能越大，同时风险也越大（股票高卖低买是最理想化的盈利方式）。
数据库数据来源可参考同项目目录下的GetData文件夹下的tuShareMain.py获取
'''
import pymysql
import pandas as pd
import config
import numpy as np
from pyecharts import Bar, Line, Overlap


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
                        t.`code`,
                        t.date,
                        t.`open`,
                        t.high,
                        t.`close`,
                        t.low,
                        t.volume
                    FROM
                        stock_history t
                    WHERE
                        t.`code` = '{0}'
                    AND t.date >= str_to_date('{1}','%Y-%m-%d')
                    AND t.date <= str_to_date('{2}','%Y-%m-%d')
                    order by t.date asc
    '''.format(code, start, end), con=connection, index_col='date')


# 分组计算
# 我们需要计算30个自然日里的股票平均波动周期。这样我们就要以30天为单位，
# 对所有的历史数据进行分组，然后逐个分组计算其波动率
def gen_item_group_index(total, group_len):
    group_count = total / group_len
    group_index = np.arange(total)
    for i in range(int(group_count)):
        group_index[i * group_len: (i + 1) * group_len] = i
    group_index[(i + 1) * group_len: total] = i + 1
    return group_index.tolist()


# 针对下跌的波动，我们把最高价设置为负数。什么是下跌的波动？就是先出现最高价，再出现最低价
def _high_price(g):
    return g.idxmin() < g.idxmax() and np.max(g) or (-np.max(g))


def view(code, start_, end_, days):
    '''
    使用pyecharts展示数据
    :param code: 股票代码
    :param start_: 开始时间（包含）格式为'2018-01-01'
    :param end_: 结束时间（包含）格式为'2018-01-01'
    :param days: 设置求多少天内波动率
    :return:
    '''
    th = get_data_from_db(code, start_, end_)
    # 中间有数据缺失的情况，因为股票在工作日才会交易，非工作日的交易数据是缺失的，我们要填充上去
    l = len(th)
    start = th.iloc[0:1].index.tolist()[0]
    end = th.iloc[l - 1:l].index.tolist()[0]
    idx = pd.date_range(start=start, end=end)

    # 接着我们使用reindex函数将缺失数据补全
    # 数据补全的规则是，价格数据用前一个交易日的数据来填充，但是交易量需要填充为0
    data = th.reindex(idx)
    zvalues = data.loc[~(data.volume > 0)].loc[:, ['volume']]
    data.update(zvalues.fillna(0))

    # pad/ffill：用前一个非缺失值去填充该缺失值
    # backfill/bfill：用下一个非缺失值填充该缺失值
    data.fillna(method='ffill', inplace=True)  #

    # 根据分组索引来分组，days为分组时间长度
    group_index = gen_item_group_index(len(data), days)
    data['group_index'] = group_index
    group = data.groupby('group_index').agg({
        'volume': 'sum',
        'low': 'min',
        'high': _high_price})

    # 添加每个分组起始日期
    data_col = pd.DataFrame({'group_index': group_index, 'date': idx})
    group['date'] = data_col.groupby('group_index').agg('first')

    # 添加波动率
    group['ripples_radio'] = group.high / group.low
    attr = [str(x.strftime('%Y-%m-%d')) for x in group.date]
    v1 = [round(x, 2) for x in group.ripples_radio.tolist()]
    v2 = [round(x / 10000, 2) for x in group.volume.tolist()]

    bar = Bar(width=1200, height=600)
    bar.add("波动率", attr, v1)

    line = Line()
    line.add("成交量", attr, v2, yaxis_formatter=" 万手")

    overlap = Overlap()
    # 默认不新增 x y 轴，并且 x y 轴的索引都为 0
    overlap.add(bar)
    # 新增一个 y 轴，此时 y 轴的数量为 2，第二个 y 轴的索引为 1（索引从 0 开始），所以设置 yaxis_index = 1
    # 由于使用的是同一个 x 轴，所以 x 轴部分不用做出改变
    overlap.add(line, yaxis_index=1, is_add_yaxis=True)
    overlap.render('股票[{0}]{1}天波动率和成交量图.html'.format(code, days))


if __name__ == "__main__":
    view(code='600306', start_='2017-01-01', end_='2018-04-16', days=30)
