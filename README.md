项目说明
===
# TuShareData
此项目主要放置TuShare数据存储及分析过程<br>

# 环境
python 3.5<br>


文件说明
===
DataAnalyse（存放数据分析过程及展示效果）
---
fluctuationRatio.py 计算股票的波动率，并且使用pyecharts展示数据<br>
showKline.py 使用从tushare下载的股票数据显示K线图<br>
![image](https://raw.githubusercontent.com/coolzhm/TuShareData/master/ProjectImag/600360%20K%E7%BA%BF%E5%9B%BE.png)


GetData(存放获取数据的方法以及数据库表创建SQL)
----
tuShareMain.py      存储获取数据的方法及使用案例<br>
stock_history.sql   创建存储股票历史数据的表<br>
stockbasics.sql     创建存储股票基本信息的表<br>

config.py
----
存储配置信息<br>

说明
---
记录此项目用到的Python相关包


备注
---
建议使用sql生成历史数据表及股票信息表，因为通过pandas.to_sql生成的表没有索引、以及字段格式不太适合后续SQL查询速度


更新日历
===
20180412 使用多线程从TuShare读取数据并存入Mysql数据库，包括股票基本信息、历史数据（两年内历史数据）、增量历史数据获取、股票分类信息<br>
20180416 计算股票的波动率，并且使用pyecharts展示数据<br>
20180425 使用从tushare下载的股票数据显示K线图<br>