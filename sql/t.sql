CREATE TABLE `sync_files` (
  `id` int(20) NOT NULL AUTO_INCREMENT,
  `uri` text,
  `md5` varchar(32) DEFAULT NULL,
  `status` int(20) DEFAULT NULL COMMENT '0--默认  200--已经同步 404--文件不存在 412--文件本地校验失败 500--目标服务器发生错误',
  `create_time` int(11) DEFAULT NULL,
  `md5_file` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=69 DEFAULT CHARSET=utf8