'''
计算股票的波动率
这里衡量股票波动指标我们定义为 最高价/最低价，比率越大我们从中获利的可能越大，同时风险也越大（股票高卖低买是最理想化的盈利方式）。
为了简单期间，我们的波动周期定为30个自然日，如果某个股票停牌，价格肯定是没有变化的，波动也为0
'''
import pymysql
import pandas as pd
import config
import numpy as np


# 从数据库获取数据
def getData():
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
                        t.`code` = '000016'
                    AND t.date > DATE_SUB(NOW(), INTERVAL 1 YEAR )
                    AND t.date < NOW()
                    order by t.date asc
    ''', con=connection, index_col='date')


th = getData()
print(th.head())
# 中间有数据缺失的情况，因为股票在工作日才会交易，非工作日的交易数据是缺失的，我们要填充上去
l = len(th)
start = th.iloc[0:1].index.tolist()[0]
end = th.iloc[l - 1:l].index.tolist()[0]
idx = pd.date_range(start=start, end=end)
print(idx)

# 接着我们使用reindex函数将缺失数据补全
# 数据补全的规则是，价格数据用前一个交易日的数据来填充，但是交易量需要填充为0
data = th.reindex(idx)
# print(th)
# print(data)
zvalues = data.loc[~(data.volume > 0)].loc[:, ['volume']]
print(zvalues)
data.update(zvalues.fillna(0))
print(data)
# pad/ffill：用前一个非缺失值去填充该缺失值
# backfill/bfill：用下一个非缺失值填充该缺失值
data.fillna(method='ffill', inplace=True)  #


# print(data)


# 分组计算
# 我们需要计算30个自然日里的股票平均波动周期。这样我们就要以30天为单位，
# 对所有的历史数据进行分组，然后逐个分组计算其波动率
def gen_item_group_index(total, group_len):
    group_count = total / group_len
    group_index = np.arange(total)
    for i in range(int(group_count)):
        group_index[i * group_len: (i + 1) * group_len] = i
    print("*****")
    print(i)
    group_index[(i + 1) * group_len: total] = i + 1
    return group_index.tolist()


print(gen_item_group_index(10, 3))
# 根据分组索引来分组
period = 30
group_index = gen_item_group_index(len(data), period)
print(group_index)
