# jzRedisRsyncTool

# 搭配logBackupTool 读取sql表自动同步文件到指定server  https://github.com/jonnywang/logBackupTool

```
CREATE TABLE `sync_files` (
  `id` int(20) NOT NULL AUTO_INCREMENT,
  `file` varchar(1024) DEFAULT NULL,
  `md5` varchar(50) DEFAULT NULL,
  `dest` varchar(10) DEFAULT NULL,
  `status` int(20) DEFAULT NULL COMMENT '0--默认  200--已经同步 404--文件不存在 412--文件本地校验失败 500--目标服务器发生错误',
  `time` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=utf8
```

```
<?xml version="1.0" encoding="UTF-8" ?>
<config>
    <address>0.0.0.0:6399</address>
    <!-- 要同步的资源所在目录 -->
    <repertory>/Users/xingqiba/workspace/go/jzRedisRsync/test/resource</repertory>
    <!-- 要同步资源的目标列表 -->
    <target>
        <server>
            <name>cdn</name>
            <address>192.168.1.123:2010</address>
        </server>
    </target>
    <!-- 数据读取配置 间隔以interval为准 -->
    <interval>10</interval>
    <mysql>
        <ip>127.0.0.1</ip>
        <username>root</username>
        <password></password>
        <port>3306</port>
        <database>data</database>
    </mysql>
</config>
```
* server的name相同时可认为为同组,与数据表字段dest相同则会被列为文件的传输目的地

# 支持redis命令同步文件
```
set server_name file    传输file到指定server_name
set server_name file ex m5sum  强制验证本地file的md5sum并传到指定server_name
```
