# switch2mqtt
permay老款智能开关服务器程序，将开关控制信息通过mqtt传递，以接入homeassistant
使用方法：
1、拷贝config.ini和s2mqtt.py到服务器
2、修改config.ini中相关配置
3、修改permay网关的服务服务器地址及端口，跟config.ini中的一致
4、运行程序（python s2mqtt.py &）
5、查看运行记录（tail -f /var/log/s2mqtt.log和s2mqtt.py同目录下生产的s2mqtt.ini文件）
6、修改configuration.yaml，写入homeassistant的配置文件
