import copy
from threading import Thread

import pymysql
import tushare as ts
from sqlalchemy import create_engine

from GetData import config


# 获取所有的股票
def getBasics():
    '''
    code,代码
    name,名称
    industry,所属行业
    area,地区
    pe,市盈率
    outstanding,流通股本(亿)
    totals,总股本(亿)
    totalAssets,总资产(万)
    liquidAssets,流动资产
    fixedAssets,固定资产
    reserved,公积金
    reservedPerShare,每股公积金
    esp,每股收益
    bvps,每股净资
    pb,市净率
    timeToMarket,上市日期
    undp,未分利润
    perundp, 每股未分配
    rev,收入同比(%)
    profit,利润同比(%)
    gpr,毛利率(%)
    npr,净利润率(%)
    holders,股东人数
    '''
    return ts.get_stock_basics()


# 获取单个股票的历史数据
def getOneHistoryData(stock_code):
    '''
    参数说明：
    code：股票代码，即6位数字代码，或者指数代码（sh=上证指数 sz=深圳成指 hs300=沪深300指数 sz50=上证50 zxb=中小板 cyb=创业板）
    start：开始日期，格式YYYY-MM-DD
    end：结束日期，格式YYYY-MM-DD
    ktype：数据类型，D=日k线 W=周 M=月 5=5分钟 15=15分钟 30=30分钟 60=60分钟，默认为D
    retry_count：当网络异常后重试次数，默认为3
    pause:重试时停顿秒数，默认为0
    返回值说明：
    date：日期
    open：开盘价
    high：最高价
    close：收盘价
    low：最低价
    volume：成交量
    price_change：价格变动
    p_change：涨跌幅
    ma5：5日均价
    ma10：10日均价
    ma20:20日均价
    v_ma5:5日均量
    v_ma10:10日均量
    v_ma20:20日均量
    turnover:换手率[注：指数无此项]
    '''
    return ts.get_hist_data(stock_code)


# 获取行业分类
def getIndustryClassified():
    '''
    获取行业分类
    返回值说明：
    code：股票代码
    name：股票名称
    c_name：行业名称
    '''
    return ts.get_industry_classified()


# 获取概念分类
def getConceptClassified():
    '''
    获取概念分类
    返回值说明：
    code：股票代码
    name：股票名称
    c_name：概念名称
    '''
    return ts.get_concept_classified()


# 获取地域分类
def getAreaClassified():
    '''
    获取地域分类。按地域对股票进行分类，即查找出哪些股票属于哪个省份。
    参数说明：
    file_path:文件路径，默认为None即由TuShare提供，可以设定自己的股票文件路径。
    返回值说明：
    code：股票代码
    name：股票名称
    area：地域名称
    '''
    return ts.get_area_classified()


# 获取中小板分类
def getSmeClassified():
    '''
    获取中小板分类。获取中小板股票数据，即查找所有002开头的股票
    参数说明：
    file_path:文件路径，默认为None即由TuShare提供，可以设定自己的股票文件路径。
    返回值说明：
    code：股票代码
    name：股票名称
    '''
    return ts.get_sme_classified()


# 获取创业板分类
def getGemClassified():
    '''
    获取创业板分类。获取创业板股票数据，即查找所有300开头的股票
    参数说明：
    file_path:文件路径，默认为None即由TuShare提供，可以设定自己的股票文件路径。
    返回值说明：
    code：股票代码
    name：股票名称
    '''
    return ts.get_gem_classified()


# 风险警示板
def getStClassified():
    '''
    风险警示板分类
    获取风险警示板股票数据，即查找所有st股票
    参数说明：
    file_path:文件路径，默认为None即由TuShare提供，可以设定自己的股票文件路径。
    返回值说明：
    code：股票代码
    name：股票名称
    '''
    return ts.get_st_classified()


# 沪深300当前成份股及所占权重
def getHs300():
    '''
    获取沪深300当前成份股及所占权重
    返回值说明：
    code :股票代码
    name :股票名称
    date :日期
    weight:权重
    '''
    return ts.get_hs300s()


# 上证50成份股
def getSz50():
    '''
    获取上证50成份股
    返回值说明：
    code：股票代码
    name：股票名称
    '''
    return ts.get_sz50s()


# 中证500成份股
def getZz500():
    '''
    获取中证500成份股
    返回值说明：
    code：股票代码
    name：股票名称
    '''
    return ts.get_zz500s()


# 终止上市的股票
def getTerminated():
    '''
    获取已经被终止上市的股票列表，数据从上交所获取，目前只有在上海证券交易所交易被终止的股票。
    返回值说明：
    code：股票代码
    name：股票名称
    oDate:上市日期
    tDate:终止上市日期
    '''
    return ts.get_terminated()


# 暂停上市的股票
def getSuspended():
    '''
    获取被暂停上市的股票列表，数据从上交所获取，目前只有在上海证券交易所交易被终止的股票。
    返回值说明：
    code：股票代码
    name：股票名称
    oDate:上市日期
    tDate:暂停上市日期
    '''
    return ts.get_suspended()


# ---------- 保存所有股票基础信息 ----------------#
# 保存所有股票基础信息
def saveStockbasic():
    # 先清空stockbasics表数据
    str = deleteStockbasic()
    if str == "OK":
        th = getBasics()
        mysql_info = config.mysql_info
        engine = create_engine('mysql://%s:%s@%s/%s?charset=%s' % (mysql_info['user'], mysql_info['passwd'],
                                                                   mysql_info['host'], mysql_info['db'],
                                                                   mysql_info['charset']))
        th.to_sql("stockbasics", engine, if_exists='append', chunksize=1000)
    else:
        print("清空数据失败了，详情:{0}".format(str))


# 清空股票基础信息
def deleteStockbasic():
    connection = pymysql.connect(**config.mysql_info)
    try:
        with connection.cursor() as cursor:
            sql = ' delete from stockbasics '
            cursor.execute(sql)
            connection.commit()  # 删除数据必须要提交
            return 'OK'
    except Exception as e:
        return 'NG:%s' % (e)
    finally:
        connection.close()


# ---------- 保存所有股票基础信息 ----------------#

# ----------- 保存数据到表 ---------------#
def saveData(table, metho):
    '''
    通过传入表名、方法名存储数据到数据库
    :param table: 表的名字
    :param metho: 方法名字
    :return:
    '''
    str = deleteAllData(table)
    # str = "OK"
    if str == "OK":
        th = metho()
        mysql_info = config.mysql_info
        engine = create_engine('mysql://%s:%s@%s/%s?charset=%s' % (mysql_info['user'], mysql_info['passwd'],
                                                                   mysql_info['host'], mysql_info['db'],
                                                                   mysql_info['charset']))
        th.to_sql(table, engine, if_exists='append', chunksize=1000)
    else:
        print("清空表[{1}]数据失败了，详情:{0}".format(str, table))


# 删除指定表格所有信息
def deleteAllData(tablename):
    connection = pymysql.connect(**config.mysql_info)
    try:
        with connection.cursor() as cursor:
            sql = ' delete from {0} '.format(tablename)
            cursor.execute(sql)
            connection.commit()  # 删除数据必须要提交
            return 'OK'
    except Exception as e:
        return 'NG:%s' % (e)
    finally:
        connection.close()


# ----------- 保存数据到表 ---------------#



# ---------- 保存所有股票近两年的历史数据 ----------------#
# 存储失败列表
fail = []
stock_list = []


# 保存所有股票近两年的历史数据
def saveStockHistory(num):
    '''
    由于数据量比较大，使用多线程进行数据同步
    :param num: 线程数量
    '''
    th = getBasics()
    th = th.T
    # 清空列表
    global fail, stock_list
    fail.clear()
    stock_list.clear()

    stock_list = copy.deepcopy(th.columns.values.tolist())
    for i in range(num):
        t = Thread(target=saveStockHistorySingle)
        t.start()


# 根据列表存储股票历史数据
def saveStockHistorySingle():
    '''
    根据tushare的方法get_hist_data去获取数据
    对于get_hist_data获取不到的数据我们将使用新接口get_k_data去获取
    :return:
    '''
    global fail, stock_list
    while len(stock_list) > 0:
        print("列表剩余[{0}]个元素".format(len(stock_list)))
        # 取出列表中的某个元素
        one = stock_list.pop()
        try:
            print("[{0}]使用get_hist_data方法获取数据".format(one))
            '''
            参数说明：
            code：股票代码，即6位数字代码，或者指数代码（sh=上证指数 sz=深圳成指 hs300=沪深300指数 sz50=上证50 zxb=中小板 cyb=创业板）
            start：开始日期，格式YYYY-MM-DD
            end：结束日期，格式YYYY-MM-DD
            ktype：数据类型，D=日k线 W=周 M=月 5=5分钟 15=15分钟 30=30分钟 60=60分钟，默认为D
            retry_count：当网络异常后重试次数，默认为3
            pause:重试时停顿秒数，默认为0
            返回值说明：
            date：日期
            open：开盘价
            high：最高价
            close：收盘价
            low：最低价
            volume：成交量
            price_change：价格变动
            p_change：涨跌幅
            ma5：5日均价
            ma10：10日均价
            ma20:20日均价
            v_ma5:5日均量
            v_ma10:10日均量
            v_ma20:20日均量
            turnover:换手率[注：指数无此项]
           '''
            oneth = ts.get_hist_data(one)
            columnslist = oneth.columns.values.tolist()
            columnslist.insert(0, 'code')
            columnslist.insert(0, 'date')
            oneth = oneth.reindex(columns=columnslist)
            oneth['code'] = one
            oneth['date'] = oneth.index
            mysql_info = config.mysql_info
            engine = create_engine('mysql://%s:%s@%s/%s?charset=%s' % (mysql_info['user'], mysql_info['passwd'],
                                                                       mysql_info['host'], mysql_info['db'],
                                                                       mysql_info['charset']))
            oneth.to_sql("stock_history", engine, index=False, if_exists='append', chunksize=1000)
        except:
            try:
                print("[{0}]使用get_k_data方法获取数据".format(one))
                oneth = ts.get_k_data(one)
                columnslist = oneth.columns.values.tolist()
                oneth = oneth.reindex(columns=columnslist)
                mysql_info = config.mysql_info
                engine = create_engine('mysql://%s:%s@%s/%s?charset=%s' % (mysql_info['user'], mysql_info['passwd'],
                                                                           mysql_info['host'], mysql_info['db'],
                                                                           mysql_info['charset']))
                oneth.to_sql("stock_history", engine, index=False, if_exists='append', chunksize=1000)
            except Exception as e:
                print("[{0}]存储失败,[{1}]".format(one, e))
                fail.append(one)
    print('当前列表已经空了[{0}]'.format(len(stock_list)))


# ---------- 保存所有股票近两年的历史数据 ----------------#


# ------------ 增量更新历史数据-------------#
# 存储失败列表
fail_increase = []
stock_list_increase = []


def saveStockHistoryIncrease(num):
    # 获取所有股票信息
    th = getBasics()
    th = th.T
    # 清空列表
    global fail_increase, stock_list_increase
    fail_increase.clear()
    stock_list_increase.clear()
    # 获取股票列表
    stock_list_increase = copy.deepcopy(th.columns.values.tolist())
    for i in range(num):
        t = Thread(target=saveStockHistoryIncreaseSingle)
        t.start()


# 获取增量股票历史数据
def saveStockHistoryIncreaseSingle():
    '''
    :return:
    '''
    global fail_increase, stock_list_increase
    while len(stock_list_increase) > 0:
        print("列表剩余[{0}]个元素".format(len(stock_list)))
        # 取出列表中一个元素
        one = stock_list_increase.pop()
        startdate, enddate = getStockStartDate(one)
        try:
            print("[{0}]使用get_hist_data方法获取数据".format(one))
            oneth = ts.get_hist_data(code=one, start=startdate, end=enddate)
            columnslist = oneth.columns.values.tolist()
            # 添加code列
            columnslist.insert(0, 'code')
            #
            columnslist.insert(0, 'date')
            oneth = oneth.reindex(columns=columnslist)
            oneth['code'] = one
            oneth['date'] = oneth.index
            mysql_info = config.mysql_info
            engine = create_engine('mysql://%s:%s@%s/%s?charset=%s' % (mysql_info['user'], mysql_info['passwd'],
                                                                       mysql_info['host'], mysql_info['db'],
                                                                       mysql_info['charset']))
            oneth.to_sql("stock_history", engine, index=False, if_exists='append', chunksize=1000)
        except:
            try:
                print("[{0}]使用get_k_data方法获取数据".format(one))
                oneth = ts.get_k_data(code=one, start=startdate, end=enddate)
                columnslist = oneth.columns.values.tolist()
                oneth = oneth.reindex(columns=columnslist)
                mysql_info = config.mysql_info
                engine = create_engine('mysql://%s:%s@%s/%s?charset=%s' % (mysql_info['user'], mysql_info['passwd'],
                                                                           mysql_info['host'], mysql_info['db'],
                                                                           mysql_info['charset']))
                oneth.to_sql("stock_history", engine, index=False, if_exists='append', chunksize=1000)
            except Exception as e:
                print("[{0}]存储失败,[{1}]".format(one, e))
                fail_increase.append(one)

    print("执行完毕，列表中元素个数为：[{0}]".format(len(stock_list_increase)))


# 根据股票代码获取股票历史中最新的日期
def getStockStartDate(stockcode):
    '''
    根据股票代码获取股票历史中最新的日期
    需要注意的是，因为接口中返回数据包括start参数那天的值，所以在数据库中查询stardate作为start参数的时候需要加1天
    :param stockcode: 股票代码
    :return: 返回两个参数分别可作为start，end参数
    '''
    connection = pymysql.connect(**config.mysql_info)
    try:
        with connection.cursor() as cursor:
            sql = ''' 
                    SELECT
                        IFNULL(
		                    DATE_ADD(max(t.date),INTERVAL 1 DAY),
                            DATE_FORMAT(
                                DATE_SUB(NOW(), INTERVAL 7 DAY),
                                '%Y-%m-%d'
                            )
                        ) startdate,
                        DATE_FORMAT(NOW(), '%Y-%m-%d') enddate
                    FROM
                        stock_history t
                    WHERE
                        t.`code` = '{0}'
                '''.format(stockcode)
            cursor.execute(sql)
            results = cursor.fetchall()
            return results[0][0], results[0][1]
    except:
        return "NG", "NG"


# ------------ 增量更新历史数据-------------#
a = [getIndustryClassified, getConceptClassified, getAreaClassified, getSmeClassified,
     getGemClassified, getStClassified, getTerminated, getSuspended]
# a = [getIndustryClassified, getConceptClassified, getAreaClassified, getSmeClassified,
#      getGemClassified, getStClassified, getHs300, getSz50, getZz500, getTerminated, getSuspended]

if __name__ == '__main__':
    # 获取股票基本数据
    # saveStockbasic()

    # 首次获取股票两年内历史数据，参数为线程数量
    saveStockHistory(8)

    # 获取股票增量历史数据，参数为线程数量
    #saveStockHistoryIncrease(1)

    # 获取股票行业分类、概念分类、地域分类、中小板分类、创业板分类等分类信息
    # 此处使用python的高阶函数，将函数名存储入列表后依次调用
    # for one in a:
    #     saveData("T_" + str(one.__name__)[3:], one)
    # test("stockbasics", getBasics)


