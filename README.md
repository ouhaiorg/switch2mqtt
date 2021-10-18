# switch2mqtt
permay老款智能开关服务器程序，将开关控制信息通过mqtt传递，以接入homeassistant
使用方法：
1、拷贝config.ini和s2mqtt.py到服务器
我的服务为路由器openwrt(192.168.2.1)，安装python及相关插件,文件拷贝到/etc/ppp/bin
2、修改config.ini中相关配置
3、修改permay网关的服务服务器地址及端口，跟config.ini中的一致
4、ssh进入服务器，cd /etc/ppp/bin;python /etc/ppp/bin/s2mqtt.py 2>&1 >/var/log/s2mqtt.log &
