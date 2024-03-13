#-* -coding: UTF-8 -* -
import time,sys,errno,time,socket,threading,json
import paho.mqtt.client as mqtt
import os,configparser,logging,logging.handlers

DEVICE_NUM = 0
SERIAL_NUM = 0


f_name = sys.argv[0].split('/')[-1].split('.')[0]
f_dir = os.path.abspath((os.path.dirname(__file__)))

rfh = logging.handlers.RotatingFileHandler(
    filename = '/var/log/' + f_name + '.log',
    mode = 'a',
    maxBytes = 20 * 1024 * 1024,
    backupCount = 3,
    encoding = None,
    delay = 0
)

logging.basicConfig(
    level = logging.DEBUG,
    handlers = [rfh],
    datefmt = "%y-%m-%d %H:%M:%S",
    format = "%(asctime)s - %(levelname)s: %(message)s"
)

def log(strLog):
   logger = logging.getLogger('main')
   logger.info(strLog)

def find_key_by_value(d, value):
  return [k for k, v in d.items() if v == value]

class mqttThread(threading.Thread):
    def __init__(self,host,port,user,password):
        threading.Thread.__init__(self)
        self.host = host 
        self.port = port
        self.pipe = None
        self.local_num = 0
        self.area_num = -1 
        self.cp = configparser.ConfigParser()
        self.threadid = threading.get_ident()
        self.cp.add_section('LOCAL_LINK')
        self.cp.add_section('AREA_LINK')
        self.client = mqtt.Client()
        self.client.username_pw_set(user,password)
        self.client.on_message = self.on_message_come # 消息到来处理函数
        self.client.on_connect = self.on_connect
        log("New MQTT Pipe[%d] create." %self.getident())
   
    def run(self):
        self.threadid = threading.get_ident()
        self.client.connect(self.host, self.port,60)
        self.client.loop_forever()
    
    def on_connect(self, client, userdata, flags, rc):
        self.client.subscribe("switch2mqtt/permay/config/#", 1)
        log("conncet MQTT [%s:%d] with code:%s." %(self.host,self.port,str(rc)))
         
    def send(self, topic, data):
        self.client.publish(topic, data, 0)
        log("Send MQTT:%s." %data)
    
    def close(self):
        self.client.disconnect()
 
    def setPipe(self,pipe):
        self.pipe = pipe

    def getident(self):
       return self.threadid

    def activecount(self):
       return threading.active_count()
           
    def on_message_come(self, client, userdata, msg):
        global DEVICE_NUM
        global SERIAL_NUM
        topic = msg.topic
        payload = msg.payload
        plen = len(payload)
        if plen > 16:
           log("payload=%s" %payload)
           str_payload = payload.decode('utf-8')
           list_payload = eval(str_payload)
           mykey = list_payload[0]
           del list_payload[0]
           myvalue = str(list_payload) 
           mykey_num = "%04x"  %(len(self.cp['AREA_LINK']))
           self.cp.set('AREA_LINK',mykey_num + " " + mykey,myvalue)
           log("key:%s;value:%s" %(mykey,myvalue))
        elif plen == 16:
             dict_payload = json.loads(payload.decode('utf-8').replace("'","\""))
             mykey = next(iter(dict_payload))
             myvalue = dict_payload[mykey]
             mykey_num = "%04x"  %(len(self.cp['LOCAL_LINK']))
             self.cp.set('LOCAL_LINK',mykey_num + " " + mykey,myvalue)
             log("key:%s;value:%s" %(mykey,myvalue))
        elif plen == 4: 
           data = bytes.fromhex(payload.decode('utf-8'))
           if self.area_num >=0:
              self.area_num = data[0]*256 + data[1]
#              self.cp.set('AREA_LINK','num',str(self.area_num))
              log("area_num = %d" %self.area_num)
           else:
              self.local_num = data[0]*256 + data[1]
 #             self.cp.set('LOCAL_LINK','num',str(self.local_num))
              log("local_num = %d" %self.local_num)
     #   log("MQTT Message Received,Topic:"+ msg.topic + ";Payload:"+ msg.payload.decode('utf-8'))
        

if __name__=='__main__':

   config_file = f_dir + "/config.ini"
   if not os.path.isfile(config_file):
       log("Error: %s does not exist!" % config_file)
       sys.exit(-1)
   cf=configparser.ConfigParser("")
   cf.read(config_file)
   mhost=cf.get("MQTT","host")
   mport=cf.getint("MQTT","port")
   muser=cf.get("MQTT","user")
   mpassword=cf.get("MQTT","password")

   mymqtt = mqttThread(mhost,mport,muser,mpassword)
   mymqtt.start()
   log("mqtt pipe id:%d" %mymqtt.getident())
   time.sleep(3) 
   mymqtt.send("switch2mqtt/permay/control","[aa00084e8d083239]")
   
   time.sleep(3)
   if mymqtt.local_num > 0: 
         for i in range(mymqtt.local_num):
             str_data = "[aa00094d8c0955%02x]" %i
             mymqtt.send("switch2mqtt/permay/control",str_data)
             time.sleep(0.1)
   time.sleep(3)
   mymqtt.area_num = 0 
   mymqtt.send("switch2mqtt/permay/control","[aa00084e91083334]")
   time.sleep(3)
   if mymqtt.area_num > 0:
      for i in range(mymqtt.area_num):
         str_data = "[aa000a4c8e09e000%02x]" %i
         mymqtt.send("switch2mqtt/permay/control",str_data)
         time.sleep(0.2)
   time.sleep(3)
   log("over and write config file") 
   filea = open(f_dir + "/" + f_name + ".ini",'w')
   mymqtt.cp.write(filea)
   filea.close()
   mymqtt.close()
   exit(0)
