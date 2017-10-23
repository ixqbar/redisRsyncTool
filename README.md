# jzRedisRsyncTool

# 搭配logBackupTool 读取sql表自动同步文件到指定server  https://github.com/jonnywang/logBackupTool

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
        <ip>192.168.1.42</ip>
        <username>root</username>
        <password>8ik,lp-</password>
        <port>3306</port>
        <database>admin_zwj2</database>
    </mysql>
</config>
```
