#-* -coding: UTF-8 -* -
import sys,errno,time,socket,threading,json
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

class pipeThread(threading.Thread):
#    '''
#    classdocs
#    '''
    def __init__(self,source,mqtt):
#        '''
#        Constructor
#        '''
        threading.Thread.__init__(self)
        self.source=source
        self.shost = None
        self.sport = None
        self.mqtt = mqtt
        self.reply_num = 0
        self.type = [0x193d,0x183e,0x173f,0x1640,0x1145,0x1244,0x1343]
        self.data = bytes()
        self.config = False
        self.com = 'SWITCH'
        self.dtype_local = 0x2d29
        self.cp = configparser.ConfigParser()
        self.cp.add_section('TYPE')
        self.cp.add_section('SWITCH')
        self.cp.add_section('AREA')
        self.cp.add_section('LOCAL')
        self.cp.set('TYPE','name','PERMAY')
        self.threadid = threading.get_ident()
        self._stop_event = threading.Event()
        (self.shost,self.sport) = self.source.getpeername()
        log("New Pipe[%d] create:(%s:%d)." % (self.getident(), self.shost,self.sport))
    
    def refresh(self,dtype):
        global DEVICE_NUM
        global SERIAL_NUM
        if dtype in self.type:
#            log('SERIAL_NUM=%x;DEVICE_NUM=%02x;type=%04x;REQUEST_NUM=%d' %(SERIAL_NUM,DEVICE_NUM,dtype,self.reply_num))
            send_data = {"num":0}
            send_data['num']="%02x"%DEVICE_NUM
            bMqtt = True
            for i in range(len(self.data)):
                send_data['s'+str(i)] = self.data[i]
                if self.data[i] > 1:
                    bMqtt = False
            if bMqtt:        
                self.mqtt.send("switch2mqtt/permay/%02x"%DEVICE_NUM, json.dumps(send_data))
            else:
                self.mqtt.send("switch2mqtt/permay/config", self.data.hex())
            if self.reply_num > 0:
                data2= bytearray.fromhex("aa00094da1166900e0")
                SERIAL_NUM = (SERIAL_NUM + 1) & 0xffff
                DEVICE_NUM = DEVICE_NUM + 1
                if DEVICE_NUM >= int(self.cp['SWITCH']['num']):
                    DEVICE_NUM = 0
                self.reply_num = self.reply_num - 1
                data2[5:7] = SERIAL_NUM.to_bytes(length=2,byteorder='big')
                data2[7] = DEVICE_NUM
                data2[8]=(0x035f-data2[5]-data2[6]-data2[7])&0xff
   #             log("Resend get Next:%s" %data2.hex())
                self.source.send(data2)
        else:
            self.mqtt.send("switch2mqtt/permay/config", self.data.hex())
            
    def getconf(self):
        global SERIAL_NUM
        global DEVICE_NUM
        switch_num = 40 
        SERIAL_NUM = (SERIAL_NUM +1 )& 0xffff
        self.config = True 
        self.com = 'SWITCH'
        time.sleep(2)
        self.send_ack(b'\x08\x4e\x93',SERIAL_NUM,0)
        time.sleep(2)
        DEVICE_NUM = 0
        log("begin to get SWITCH")
        if (self.cp.has_option('SWITCH','num') and int(self.cp['SWITCH']['num'])>1):
            switch_num = int(self.cp['SWITCH']['num'])
        self.send_ack(b'\x09\x4d\x92',SERIAL_NUM,DEVICE_NUM)
        time.sleep(int(switch_num/4))
        self.com = 'AREA'
        log("begin to get AREA")
        self.send_ack(b'\x08\x4e\x91',SERIAL_NUM,0)
        time.sleep(2)
        DEVICE_NUM = 0
    #    self.send_ack(b'\x09\x4d\x90',SERIAL_NUM,DEVICE_NUM)
        self.send_ack(b'\x0a\x4c\x90',SERIAL_NUM,DEVICE_NUM)
        time.sleep(int(switch_num/2))
        stype = {'0000':2,'0100':1,'0401':2,'0202':4,'0502':3,'0302':2,'0602':1}
        self.cp.set('LOCAL',"num", "0")
        snum = 4
        local_num = 0
        for ii in range(int(self.cp['SWITCH']['num'])):
           for yy in range(stype[self.cp['TYPE']["%04x"%ii]]):
              local_num += 1
              snum -= 1
              self.cp.set('LOCAL',"%04x"%snum,"%02x%02x"%(ii,yy))
              if snum % 4 == 0:
                 snum += 8
              self.cp.set('LOCAL',"num",str(local_num))
              self.dtype_local = (int((local_num+3)/4)+16)*256 + 0x46 - int((local_num+3)/4)
        self.cp.write(open(f_dir + "/" + f_name + ".ini",'w'))
        log("write config file over.")
        self.com = 'SWITCH'
        self.config = False 
        
    def send_ack(self,command,serial,num):
        data2= bytearray.fromhex("aa00084e93083134")
        data2[2:5] = command
        data2[5:7] = serial.to_bytes(length=2,byteorder='big')
        if data2[2]==0x0a:
            data2 =data2 +b'\x00\x00'
            data2[7:9]=num.to_bytes(length=2,byteorder='big')
            data2[9] = (0x0756-data2[2]-data2[3]-data2[4]-data2[5]-data2[6]-data2[7]-data2[8])&0xff
        if data2[2]==9:
            data2 =data2 +b'\x00'
            data2[7]=num
            data2[8] = (0x0656-data2[2]-data2[3]-data2[4]-data2[5]-data2[6]-data2[7])&0xff
        elif data2[2]==8:    
            data2[7] = (0x0556-data2[2]-data2[3]-data2[4]-data2[5]-data2[6])&0xff
        log("Send to get status:%s" %data2.hex())
        self.source.send(data2)      
    
    def close(self):
       self._stop_event.set()
       log("close func,set bRun as False")
 
    def stop(self):
       self._stop_event.set()

    def stopped(self):
       return self._stop_event.is_set()
    
    def getident(self):
       return self.threadid

    def activecount(self):
       return threading.active_count()
 
    def run(self):
        global DEVICE_NUM
        global SERIAL_NUM
        last_data = b''
        self.threadid = threading.get_ident()
        while not self.stopped():
            try:
                data=self.source.recv(1024)
                if not data: break
                data2 = last_data + data
                if data2[0:2] != b'\xaa\x00':
                   data2 = data
                   if data2[0:2] != b'\xaa\x00':
                      lastdata = ''
                      continue
                if  len(data2) < 4 or len(data2) < data2[2] :
                    last_data = data2
                    continue
                if len(data2) > data2[2] and data2[2] > 4:
                    last_data = data2[data2[2]:len(data2)]
                    data2 = data2[0:data2[2]]
                else:       
                    last_data = b''       
                data = data2 
                dtype = data[2]*256+data[3]
                ctype = data[4]
                log("Data Received: %s."  %data.hex())
          #      log("Data Received: %s;dtype=%04x,ctype=%02x"  %(data.hex(),dtype,ctype))
                if ctype == 0x12:
                    if dtype == 0x1145:
                        log("Heartbeat package received.")
                        if(self.config and self.cp.has_option('LOCAL','num') and int(self.cp['LOCAL']['num'])>0):
                            self.config = False
                elif ctype == 0x18:
                    if dtype == 0x1046:
             #           log("DEVICE=%x;SERIAL=%x" %(DEVICE_NUM,SERIAL_NUM))
                        DEVICE_NUM =  data[13]*256 + data[14] 
                        SERIAL_NUM = (SERIAL_NUM + 1) & 0xffff
                        data2= bytearray.fromhex("aa000a4c8e0f2e001500")
                        data2[5:7]=SERIAL_NUM.to_bytes(length=2,byteorder='big')
                        data2[7:9]=DEVICE_NUM.to_bytes(length=2,byteorder='big')
                        data2[9]=(0x0472-data2[5]-data2[6]-data2[7]-data2[8])&0xff
                        log("Send to get status:%s" %data2.hex())
                        self.source.send(data2)
                elif ctype == 0x16:
                    if dtype == 0x1046:
                        DEVICE_NUM = data[14] 
                        SERIAL_NUM = (SERIAL_NUM + 1) & 0xffff
                        data2= bytearray.fromhex("aa00094d8c05400b24")
                        data2[5:7] = SERIAL_NUM.to_bytes(length=2,byteorder='big')
                        data2[7] = DEVICE_NUM
                        data2[8] = (0x0374-data2[5]-data2[6]-data2[7])&0xff
                        log("Send to get status:%s" %data2.hex())
                        self.source.send(data2)  
                elif ctype == 0x04 or ctype == 0x0b:
                    if dtype == 0x1244:
                        if self.config:
                            log("config....,exit")
                            continue
                        self.reply_num = int(self.cp['SWITCH']['num']) + 1
            #            log("SERIAL_NUM=%x,data[13-14]=%s" %(SERIAL_NUM,data[13:15].hex()))
                        device = data[13:15].hex()
                        for ii in range(int(self.cp['SWITCH']['num'])):
                           if device == self.cp['SWITCH']["%04x"%ii]:
                                 DEVICE_NUM = ii
                                 break
                        data2= bytearray.fromhex("aa00094da1166900e0")
                        data2[7] = DEVICE_NUM
                        data2[8]=(0x035f-data2[5]-data2[6]-data2[7])&0xff
                        self.reply_num = self.reply_num - 1
                        SERIAL_NUM = data2[5]*256+data2[6] 
                        log("Send to get status:%s" %data2.hex())
                        self.source.send(data2)
                        self.mqtt.send("switch2mqtt/permay/click/%02x"%DEVICE_NUM + "/%02x"%data[15],str(data[16]))
                elif ctype== 0x09:
                    serial_num = data[13]*256+data[14]
                    if SERIAL_NUM != serial_num:
                       if dtype == 0x1046:
                          self.reply_num = int(self.cp['SWITCH']['num']) + 1
                       log("SERIAL_NUM=%04x != serial_num,continue" %SERIAL_NUM)  
                       continue
                    serial_num =  (serial_num + 1)& 0xffff
                    if self.config and dtype == 0x1244:
                       data2 = data[15:data[2]-1]
                       log("get config;%s number=%s" %(self.com, data2.hex()))
                       if self.com == 'SWITCH' or self.com  == 'AREA':
                          self.cp.set(self.com,'num',str(data2[0]*256+data2[1]))
                          self.mqtt.send("switch2mqtt/permay/config/number/%s"%self.com, str(data2[0]*256+data2[1]))
                       SERIAL_NUM = serial_num
                    elif dtype == 0x1e38:
                       data2 = data[15:data[2]-1]   
                       log('get_switch;data=%s' %data2.hex())
                       self.cp.set('TYPE',"%04x"%DEVICE_NUM,data2[10:12].hex())
                       self.cp.set('SWITCH',"%04x"%DEVICE_NUM,data2[9:10].hex()+data2[8:9].hex())
                       DEVICE_NUM = DEVICE_NUM +1
                       SERIAL_NUM = (serial_num +1)&0xffff
                       if(self.config and DEVICE_NUM < int(self.cp['SWITCH']['num'])):
                          self.send_ack(b'\x09\x4d\x92',SERIAL_NUM,DEVICE_NUM)
                       else:
                          self.mqtt.send("switch2mqtt/permay/config/switch", data2.hex())
                    elif dtype == 0x1442:
                       data2 = data[15:data[2]-1]   
                       log('get_switch_area;data=%s' %data2.hex())  
                       self.cp.set('AREA',data2[2:4].hex(),data2[0:2].hex())
                       DEVICE_NUM = DEVICE_NUM +1
                       SERIAL_NUM = (serial_num +1)&0xffff
                       if(self.config and DEVICE_NUM < int(self.cp['AREA']['num'])):
                          self.send_ack(b'\x0a\x4c\x90',SERIAL_NUM,DEVICE_NUM)
                       else:
                          self.mqtt.send("switch2mqtt/permay/config/area", data2.hex())
                    elif dtype == 0x1046:
                        if self.config:
                            log("config....,exit")
                            continue
           #             log("104609 SERIAL_NUM=%d,data[13-14]=%d" %(SERIAL_NUM,data[13]*256+data[14]))
                        self.reply_num = int(self.cp['SWITCH']['num']) + 1 
                        data2= bytearray.fromhex("aa00094da1166900e0")
                        data2[5:7] = serial_num.to_bytes(length=2,byteorder='big')
                        data2[7] = DEVICE_NUM
                        data2[8]=(0x035f-data2[5]-data2[6]-data2[7])&0xff
                        self.reply_num = self.reply_num - 1
                        SERIAL_NUM = serial_num
                        log("Send get Next:%s" %data2.hex())
                        self.source.send(data2)
                    elif dtype == 0x1442:
                        serial_num =  data[13]*256+data[14]
                        data_a = {}
                        data_a[data[15:17].hex()] = data[17:19].hex()
                        self.mqtt.send("switch2mqtt/permay/config/local", str(data_a))
                    elif dtype == self.dtype_local: #area_num=116:2c2a,area_num=124:2d29
                        serial_num =  data[13]*256+data[14] 
                        data2 = data[15:data[2]-1]
                        log("local_link_show;data=%s" %data2.hex())
                        data_a = []
                        key_a = "%04x" %DEVICE_NUM
                        if self.cp.has_option('AREA',key_a):
                           data_a.append(self.cp['AREA'][key_a])
                        else:
                           data_a.append(key_a)
                           log("DEVICE_NUM=%04x" %DEVICE_NUM)
                        for ii in range(len(data2)):
                            for yy in range(4):
                                ss = (data2[ii] >> (6-yy*2))& 0x3
                                if ss >1:
                                   data_a.append("%s11" %self.cp['LOCAL']["%04x" %(ii*4+yy)])
                                elif ss ==1:    
                                   data_a.append("%s01" %self.cp['LOCAL']["%04x" %(ii*4+yy)])
                        self.mqtt.send("switch2mqtt/permay/config/area", str(data_a))        
                    else:
                        self.data = data[15:data[2]-1]   
                        self.refresh(dtype)
                else:
                   log("CTYPE ELSE:ctype=%02x;dtype=%04x" %(ctype,dtype))
            except Exception as ex:
                log("redirect error:"+str(ex))

class mqttThread(threading.Thread):
    def __init__(self,host,port,user,password,sock):
        threading.Thread.__init__(self)
        self.host = host 
        self.port = port
        self.sock = sock
        self.pipe = None 
        self.sock_retry = 0
        self.threadid = threading.get_ident()
        self.client = mqtt.Client()
        self.client.username_pw_set(user,password)
        self.client.on_message = self.on_message_come # 消息到来处理函数
        self.client.on_connect = self.on_connect
        log("New MQTT Pipe[%d] create:%s." % (self.getident(),self.sock.getpeername()))
   
    def run(self):
        self.threadid = threading.get_ident()
        self.client.connect(self.host, self.port,60)
        self.client.loop_forever()
    
    def on_connect(self, client, userdata, flags, rc):
        self.client.subscribe("switch2mqtt/permay/control/#", 1)
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
           
    def sock_send(self, sock, data):
       try:
          sock.send(data) 
          log("Redirect MQTT:%s,pipe id=%d" %(data.hex(),self.pipe.getident()))
          pipestopped = self.pipe.stopped()
          if pipestopped > 0:
            log("pipe if stopped:%d" %pipestopped)
          self.sock_retry = 0
       except Exception as e:
             log("Redirect MQTT IOError:%s" %str(e))
             log("pipe threading count:%d" %self.pipe.activecount())
             self.sock_retry += 1
             log("sock_retry=%d,pipe id=%d" %(self.sock_retry,self.pipe.getident()))
             if self.sock_retry >3:
                log("close sock and all threading........................")
                self.pipe.stop()
                self.close()
                self.sock.shutdown(socket.SHUT_RDWR)          
                self.sock.close()
           
    def on_message_come(self, client, userdata, msg):
        global DEVICE_NUM
        global SERIAL_NUM
        log("MQTT Message Received,Topic:"+ msg.topic + ";Payload:"+ msg.payload.decode('utf-8'))
        if self.sock:
            try:
                topic = msg.topic
                payload = msg.payload
                length = len(msg.payload)
                if length == 1 and len(msg.topic) == 32:
                    data = bytearray.fromhex("aa000b4b8878011c0401db")
                    SERIAL_NUM = (SERIAL_NUM + 1) & 0xffff                  
                    data[5:7]=SERIAL_NUM.to_bytes(length=2,byteorder='big')
                    data[7:8]=bytes.fromhex(topic.split("/")[3])
                    data[8:9]=bytes.fromhex(topic.split("/")[4])
                    data[9]=payload[0]-ord('0')
                    data[10]=(0x578-data[5]-data[6]-data[7]-data[8]-data[9])&0xff
                    DEVICE_NUM = data[7]
                    log('SERIAL_NUM=%x;DEVICE_NUM=%02x;' %(SERIAL_NUM,DEVICE_NUM))
                    time.sleep(0.02)
                    self.sock_send(self.sock,data)
                elif( length >= 10):
                   if(payload[0]== ord('[') or payload[0]== ord('{')): 
                       data = bytes.fromhex(payload.decode('utf-8')[1:length-1])
                       ii = data[2]
                       if len(data) +1 == ii:
                           d2 = 0
                           for i in range(len(data)):
                               d2 = data[i] + d2
                           d2 = (0x100-d2)&0xff
                           data = data + d2.to_bytes(length=1,byteorder='big')
                       if len(data) == ii:   
                           SERIAL_NUM = data[5]*256 + data[6]       
                           if ii<9:
                              DEVICE_NUM = 0xff
                           elif ii==9:                        
                              DEVICE_NUM = data[7]
                           else:
                              DEVICE_NUM = data[7]*256+ data[8]                            
                           log('SERIAL_NUM=%x;DEVICE_NUM=%02x' %(SERIAL_NUM,DEVICE_NUM))                       
                           self.sock_send(self.sock,data)
            except Exception as ex:
                log("Redirect MQTT error:"+str(ex))

if __name__=='__main__':

   config_file = f_dir + "/config.ini"
   if not os.path.isfile(config_file):
      log("Error: %s does not exist!" % config_file)
      sys.exit(-1)
   cf=configparser.ConfigParser("")
   cf.read(config_file)
   host=cf.get("S2MQTT","host")
   port=cf.getint("S2MQTT","port")
   mhost=cf.get("MQTT","host")
   mport=cf.getint("MQTT","port")
   muser=cf.get("MQTT","user")
   mpassword=cf.get("MQTT","password")
   sock=None
   newsock=None
   sock=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
   sock.bind((host,port))
   sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
   sock.listen(5)
   log("start listen on [%s:%d]." % (host,port))
   while True:
      newsock,address=sock.accept()
      log("new connect from [%s:%s]." %(address[0],address[1]))
      mymqtt = mqttThread(mhost,mport,muser,mpassword,newsock)
      mymqtt.start()
      log("mqtt pipe id:%d" %mymqtt.ident)
      p1 = pipeThread(newsock,mymqtt)
      p1.start()
      log("switch pipe id:%d" %p1.ident)
      mymqtt.setPipe(p1)
      p1.getconf()
