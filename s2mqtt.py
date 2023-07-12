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
        self.type = ['193d','183e','173f','1640','1145','1244','1343']
        self.data = bytes()
        self.config = False
        self.last_com = ''
        self.cp=[]
        self.threadid = threading.get_ident()
        self._stop_event = threading.Event()
        (self.shost,self.sport) = self.source.getpeername()
        log("New Pipe[%d] create:(%s:%d)." % (self.getident(), self.shost,self.sport))
    
    def refresh(self,dtype):
        global DEVICE_NUM
        global SERIAL_NUM
        if dtype in self.type:
#            log('SERIAL_NUM=%x;DEVICE_NUM=%02x;type=%s;REQUEST_NUM=%d' %(SERIAL_NUM,DEVICE_NUM,dtype,self.reply_num))
            send_data = {"num":0,"type":"0000","s0":0,"s1":0,"s2":0,"s3":0,"s4":0,"s5":0,"s6":0,"s7":0,"s8":0}
            send_data['num']="%02x"%DEVICE_NUM
            send_data['type']=dtype
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
        time.sleep(2)
        self.cp = configparser.ConfigParser()
        self.cp.read(f_dir + "/" + f_name + ".ini")
        if not self.cp.has_option('SWITCH','name'):
            log("begin to create configuration file")
            SERIAL_NUM = (SERIAL_NUM +1 )& 0xffff
            self.config = True 
            self.last_com = 'get_switch_num'
            self.send_ack(b'\x08\x4e\x93',SERIAL_NUM,0)
            log("data=%s" %self.data.hex())
            self.cp.add_section('TYPE')
            self.cp.add_section('SWITCH')
            self.cp.add_section('AREA')
            self.cp.add_section('LOCAL')
            self.cp.set('SWITCH','name','PERMAY')
        
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
                log("Data Received: %s"  %data.hex())
                dtype = data[2:4].hex()
                ctype = data[4]
                if ctype == 0x12:
                    if dtype == '1145':
                        log("receive heartbeat package.")
                        log("reply_num=%d" %self.reply_num)
                        DEVICE_NUM = data[14] 
                        SERIAL_NUM = (SERIAL_NUM + 1) & 0xffff
                        data2= bytearray.fromhex("aa00094d8c05400b24")
                        data2[5:7] = SERIAL_NUM.to_bytes(length=2,byteorder='big')
                        data2[7] = DEVICE_NUM
                        data2[8] = (0x0374-data2[5]-data2[6]-data2[7])&0xff
                        log("Send to get status:%s" %data2.hex())
                        self.source.send(data2)  
                elif ctype == 0x18:
                    if dtype == '1046':
                        log("DEVICE=%x;SERIAL=%x" %(DEVICE_NUM,SERIAL_NUM))
                        DEVICE_NUM =  data[13]*256 + data[14] 
                        SERIAL_NUM = (SERIAL_NUM + 1) & 0xffff
                        log("DEVICE=%x;SERIAL=%x" %(DEVICE_NUM,SERIAL_NUM))
                        data2= bytearray.fromhex("aa000a4c8e0f2e001500")
                        data2[5:7]=SERIAL_NUM.to_bytes(length=2,byteorder='big')
                        data2[7:9]=DEVICE_NUM.to_bytes(length=2,byteorder='big')
                        data2[9]=(0x0472-data2[5]-data2[6]-data2[7]-data2[8])&0xff
                        log("Send to get status:%s" %data2.hex())
                        self.source.send(data2)
                elif ctype == 0x16:
                    if dtype == '1046':
                        DEVICE_NUM = data[14] 
                        SERIAL_NUM = (SERIAL_NUM + 1) & 0xffff
                        data2= bytearray.fromhex("aa00094d8c05400b24")
                        data2[5:7] = SERIAL_NUM.to_bytes(length=2,byteorder='big')
                        data2[7] = DEVICE_NUM
                        data2[8] = (0x0374-data2[5]-data2[6]-data2[7])&0xff
                        log("Send to get status:%s" %data2.hex())
                        self.source.send(data2)  
                elif ctype == 0x04 or ctype == 0x0b:
                    if dtype == '1244':
                        if self.config:
                            continue
                        self.reply_num = int(self.cp['SWITCH']['num']) + 1
                        log("SERIAL_NUM=%x,data[13-14]=%s" %(SERIAL_NUM,data[13:15].hex()))
                        device = data[13:15].hex()
                        for ii in range(int(self.cp['SWITCH']['num'])):
                           if device == self.cp['TYPE']["%04x"%ii]:
                                 DEVICE_NUM = ii
                                 break
                        data2= bytearray.fromhex("aa00094da1166900e0")
                        data2[7] = DEVICE_NUM
                        data2[8]=(0x035f-data2[5]-data2[6]-data2[7])&0xff
                        self.reply_num = self.reply_num - 1
                        SERIAL_NUM = data2[5]*256+data2[6] 
                        log("Send to get status:%s" %data2.hex())
                        self.source.send(data2)
                        self.mqtt.send("switch2mqtt/permay/click/%02x"%DEVICE_NUM + "/%02x"%data[15].hen(),"1")
                elif ctype== 0x09:
                    serial_num = data[13]*256+data[14]
                    if SERIAL_NUM != serial_num:
                       if dtype == '1046':
                          self.reply_num = int(self.cp['SWITCH']['num']) + 1 
                       continue
                    serial_num =  (serial_num + 1)& 0xffff
                    if self.config:
                        if self.last_com == 'get_switch_num':
                            log('get_switch_num')
                            data2 = data[15:data[2]-1]   
                            log('data=%s' %data2.hex())
                            self.cp.set('SWITCH','num',str(data2[0]*256+data2[1]))
                            self.last_com = 'get_switch'
                            DEVICE_NUM = 0
                            SERIAL_NUM = serial_num
                            log('begin get switch,SERIAL_NUM=%d'%SERIAL_NUM)
                            self.send_ack(b'\x09\x4d\x92',SERIAL_NUM,DEVICE_NUM)
                        elif self.last_com == 'get_switch':
                            log('get_switch')
                            data2 = data[15:data[2]-1]   
                            log('data=%s' %data2.hex())
                            self.cp.set('TYPE',"%04x"%DEVICE_NUM,data2[10:12].hex())
                            self.cp.set('SWITCH',"%04x"%DEVICE_NUM,data2[9:10].hex()+data2[8:9].hex())
                            DEVICE_NUM = DEVICE_NUM +1
                            SERIAL_NUM = (serial_num +1)&0xffff
                            if(DEVICE_NUM >= int(self.cp['SWITCH']['num'])):
                                self.last_com = 'get_switch_area_num'
                                self.send_ack(b'\x08\x4e\x91',SERIAL_NUM,DEVICE_NUM)
                            else:    
                                self.send_ack(b'\x09\x4d\x92',SERIAL_NUM,DEVICE_NUM)
                        elif self.last_com == 'get_switch_area_num':
                            log('get_switch_area_num')
                            data2 = data[15:data[2]-1]   
                            log('data=%s' %data2.hex())
                            self.cp.set('AREA','num',str(data2[0]*256+data2[1]))
                            self.last_com = 'get_switch_area'
                            DEVICE_NUM = 0
                            SERIAL_NUM = serial_num
                            log('begin get switch,SERIAL_NUM=%d'%SERIAL_NUM)
                            self.send_ack(b'\x0a\x4c\x90',SERIAL_NUM,DEVICE_NUM)
                        elif self.last_com == 'get_switch_area':
                            log('get_switch_area')
                            data2 = data[15:data[2]-1]   
                            log('data=%s' %data2.hex())  
                            self.cp.set('AREA',data2[2:4].hex(),data2[0:2].hex())
                            DEVICE_NUM = DEVICE_NUM +1
                            SERIAL_NUM = (serial_num +1)&0xffff
                            if(DEVICE_NUM >= int(self.cp['AREA']['num'])):
                                self.last_com = ''
                                snum = 4
                                stype = {'0000':2,'0100':1,'0401':2,'0202':4,'0502':3,'0302':2,'0602':1}
                                for ii in range(int(self.cp['SWITCH']['num'])):
                                    for yy in range(stype[self.cp['TYPE']["%04x"%ii]]):
                                        snum = snum -1
                                        self.cp.set('LOCAL',"%04x"%snum,"%02x%02x"%(ii,yy))
                                        if snum % 4 == 0:
                                           snum = snum + 8
                                filea = open(f_dir + "/" + f_name + ".ini",'w')
                                self.cp.write(filea)
                                log("write configuration file finish")
                                filea.close()
                                self.config = False
                            else:    
                                self.send_ack(b'\x0a\x4c\x90',SERIAL_NUM,DEVICE_NUM)
                    elif dtype == '1046':
                        if self.config:
                            log("config....,exit")
           #                 continue
#                       log("SERIAL_NUM=%d,data[13-14]=%d" %(SERIAL_NUM,data[13]*256+data[14]))
                        self.reply_num = int(self.cp['SWITCH']['num']) + 1 
                        data2= bytearray.fromhex("aa00094da1166900e0")
                        data2[5:7] = serial_num.to_bytes(length=2,byteorder='big')
                        data2[7] = DEVICE_NUM
                        data2[8]=(0x035f-data2[5]-data2[6]-data2[7])&0xff
                        self.reply_num = self.reply_num - 1
                        SERIAL_NUM = serial_num
                        log("Send get Next:%s" %data2.hex())
                        self.source.send(data2)
                    elif dtype == '1442':
                        serial_num =  data[13]*256+data[14]
                        data_a = {}
                        data_a[data[15:17].hex()] = data[17:19].hex()                 
                        log(data_a)
                        self.mqtt.send("switch2mqtt/permay/config/local", str(data_a))
                    elif dtype == '2c2a':
                        serial_num =  data[13]*256+data[14] 
                        data2 = data[15:data[2]-1]
                        log("data=%s" %data2.hex())
                        data_a = {}
                        for ii in range(len(data2)):
                            for yy in range(4):
                                ss = (data2[ii] >> (6-yy*2))& 0x3
                                if ss >1:
                                    data_a[self.cp['LOCAL']["%04x"%(ii*4+yy)]]='11'
                                elif ss ==1:    
                                    data_a[self.cp['LOCAL']["%04x"%(ii*4+yy)]]='01'
                        str2 = "%04x"%DEVICE_NUM + str(data_a)          
                        log(str2)
                        self.mqtt.send("switch2mqtt/permay/config/area", str2)        
                    else:
                        self.data = data[15:data[2]-1]   
                        self.refresh(dtype)
       #         if dtype == '094da1':
       #             SERIAL_NUM = data[5]*256 + data[6]
       #             DEVICE_NUM = data[7]
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
          log("pipe if stopped:%d" %self.pipe.stopped())
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

class s2mqtt(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        config_file = f_dir + "/config.ini"
        if not os.path.isfile(config_file):
           log("Error: %s does not exist!" % config_file)
           sys.exit(-1)
        cf=configparser.ConfigParser("")
        cf.read(config_file)
        self.host=cf.get("S2MQTT","host")
        self.port=cf.getint("S2MQTT","port")
        self.mhost=cf.get("MQTT","host")
        self.mport=cf.getint("MQTT","port")
        self.muser=cf.get("MQTT","user")
        self.mpassword=cf.get("MQTT","password")
        self.sock=None
        self.newsock=None
        self.sock=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.sock.bind((self.host,self.port))
        self.sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        self.sock.listen(5)
        log("start listen on [%s:%d]." % (self.host,self.port))
    def run(self):

        while True:
            self.newsock,address=self.sock.accept()
            log("new connect from [%s:%s]." %(address[0],address[1]))
            mymqtt = mqttThread(self.mhost,self.mport,self.muser,self.mpassword, self.newsock)
            mymqtt.start()
            log("mqtt pipe id:%d" %mymqtt.getident())
            p1=pipeThread(self.newsock,mymqtt)
            p1.start()
            log("gt06 pipe id:%d" %p1.getident())
            p1.getconf()
            mymqtt.setPipe(p1)

class mqttThread2(threading.Thread):
    def __init__(self):
       threading.Thread.__init__(self)
    def run(self):
       print("aaaaaaaaaaa")
    def getident(self):
       return threading.get_ident()

if __name__=='__main__':
#    mapp = s2mqtt()
#    mapp.start()

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
      p1.getconf()
      mymqtt.setPipe(p1)
