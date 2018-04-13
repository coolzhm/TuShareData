项目说明
===
# TuShareData
此项目主要放置TuShare数据存储及分析过程

文件说明
===
GetData
----
config.py           存储配置信息<br>
tuShareMain.py      存储获取数据的方法及使用案例<br>
stock_history.sql   创建存储股票历史数据的表<br>
stockbasics.sql     创建存储股票基本信息的表<br>


说明
---
记录此项目用到的Python相关包


更新日历
===
20180412 使用多线程从TuShare读取数据并存入Mysql数据库，包括股票基本信息、历史数据（两年内历史数据）、增量历史数据获取、股票分类信息