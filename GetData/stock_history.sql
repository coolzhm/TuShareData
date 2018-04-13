CREATE TABLE `stock_history` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `date` date NOT NULL COMMENT '日期',
  `code` varchar(10) NOT NULL COMMENT '股票代码',
  `open` double DEFAULT NULL COMMENT '开盘价',
  `high` double DEFAULT NULL COMMENT '最高价',
  `close` double DEFAULT NULL COMMENT '收盘价',
  `low` double DEFAULT NULL COMMENT '最低价',
  `volume` double DEFAULT NULL COMMENT '成交量',
  `price_change` double DEFAULT NULL COMMENT '价格变动',
  `p_change` double DEFAULT NULL COMMENT '涨跌幅',
  `ma5` double DEFAULT NULL COMMENT '5日均价',
  `ma10` double DEFAULT NULL COMMENT '10日均价',
  `ma20` double DEFAULT NULL COMMENT '20日均价',
  `v_ma5` double DEFAULT NULL COMMENT '5日均量',
  `v_ma10` double DEFAULT NULL COMMENT '10日均量',
  `v_ma20` double DEFAULT NULL COMMENT '20日均量',
  `turnover` double DEFAULT NULL COMMENT '换手率[注：指数无此项]',
  PRIMARY KEY (`id`),
  KEY `history_index01` (`date`),
  KEY `history_index02` (`code`),
  KEY `history_index03` (`date`,`code`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

