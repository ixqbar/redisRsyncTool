CREATE TABLE `sync_files` (
  `id` int(20) NOT NULL AUTO_INCREMENT,
  `file` varchar(1024) DEFAULT NULL,
  `md5` varchar(50) DEFAULT NULL,
  `dest` varchar(10) DEFAULT NULL,
  `status` int(20) DEFAULT NULL COMMENT '0--默认  200--已经同步 404--文件不存在 412--文件本地校验失败 500--目标服务器发生错误',
  `time` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=utf8