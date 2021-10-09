#-* -coding: UTF-8 -* -
import time,socket,threading,json
import paho.mqtt.client as mqtt
import configparser

DEVICE_NUM = 0
SERIAL_NUM = 0

def log(strLog):
    strs=time.strftime("%Y-%m-%d %H:%M:%S")
    print(strs+"->"+strLog)
	
class pipethread(threading.Thread):
    '''
    classdocs
    '''
    def __init__(self,source,mqtt):
        '''
        Constructor
        '''
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
        (self.shost,self.sport) = self.source.getpeername()
        log("New Pipe create:(%s:%d)" % (self.shost,self.sport))
    
    def refresh(self,dtype):
        global DEVICE_NUM
        global SERIAL_NUM
        if dtype in self.type:
#            print('SERIAL_NUM=%x;DEVICE_NUM=%02x;type=%s;REQUEST_NUM=%d' %(SERIAL_NUM,DEVICE_NUM,dtype,self.reply_num))
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
                if DEVICE_NUM >= 0x28:
                    DEVICE_NUM = 0
                self.reply_num = self.reply_num - 1
                data2[5:7] = SERIAL_NUM.to_bytes(length=2,byteorder='big')
                data2[7] = DEVICE_NUM
                data2[8]=(0x035f-data2[5]-data2[6]-data2[7])&0xff
   #             print("Resend get Next:%s" %data2.hex())
                self.source.send(data2)
        else:
            self.mqtt.send("switch2mqtt/permay/config", self.data.hex())
            
    def getconf(self):
        global SERIAL_NUM
        time.sleep(2)
        self.cp = configparser.ConfigParser()
        self.cp.read("s2mqtt.ini")
        if not self.cp.has_option('SWITCH','name'):
            print("begin to create configuration file")
            SERIAL_NUM = (SERIAL_NUM +1 )& 0xffff
            self.config = True 
            self.last_com = 'get_switch_num'
            self.send_ack(b'\x08\x4e\x93',SERIAL_NUM,0)
            print("data=%s" %self.data.hex())
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
        print("Send to get status:%s" %data2.hex())
        self.source.send(data2)      
   
    def run(self):
        global DEVICE_NUM
        global SERIAL_NUM

        last_data = b''
        while True:
            try:
                data=self.source.recv(1024)
                if not data: break
                if data[0:2] != b'\xaa\x00':
                    data2 = last_data + data
                else:
                    data2 = data                
                if  data2[0:2] != b'\xaa\x00' or len(data2) < 4 or len(data2) < data2[2] :
                    last_data = data2
                    continue
                if len(data2) > data2[2]:
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
                        print("receive heartbeat package.")
                elif ctype == 0x18:
                    if dtype == '1046':
                        print("DEVICE=%x;SERIAL=%x" %(DEVICE_NUM,SERIAL_NUM))
                        DEVICE_NUM =  data[13]*256 + data[14] 
                        SERIAL_NUM = (SERIAL_NUM + 1) & 0xffff
                        print("DEVICE=%x;SERIAL=%x" %(DEVICE_NUM,SERIAL_NUM))
                        data2= bytearray.fromhex("aa000a4c8e0f2e001500")
                        data2[5:7]=SERIAL_NUM.to_bytes(length=2,byteorder='big')
                        data2[7:9]=DEVICE_NUM.to_bytes(length=2,byteorder='big')
                        data2[9]=(0x0472-data2[5]-data2[6]-data2[7]-data2[8])&0xff
                        print("Send to get status:%s" %data2.hex())
                        self.source.send(data2)
                elif ctype == 0x16:
                    if dtype == '1046':
                        DEVICE_NUM = data[14] 
                        SERIAL_NUM = (SERIAL_NUM + 1) & 0xffff
                        data2= bytearray.fromhex("aa00094d8c05400b24")
                        data2[5:7] = SERIAL_NUM.to_bytes(length=2,byteorder='big')
                        data2[7] = DEVICE_NUM
                        data2[8] = (0x0374-data2[5]-data2[6]-data2[7])&0xff
                        print("Send to get status:%s" %data2.hex())
                        self.source.send(data2)  
                elif ctype == 0x04 or ctype == 0x0b:
                    if dtype == '1244':
                        if self.config:
                            continue
                        self.reply_num =0x29
                        print("SERIAL_NUM=%d,data[13-14]=%s" %(SERIAL_NUM,data[13:15].hex()))
                        device = data2[13:15].hex()
                        for ii in range(self.cp.getint('SWITCH','num')):
                            if device == self.cp.get('SWITCH',"%04x"%ii):
                                 DEVICE_NUM = ii
                                 break
                        data2= bytearray.fromhex("aa00094da1166900e0")
                        data2[7] = DEVICE_NUM
                        data2[8]=(0x035f-data2[5]-data2[6]-data2[7])&0xff
                        self.reply_num = self.reply_num - 1
                        SERIAL_NUM = data2[5]*256+data2[6] 
                        print("Send to get status:%s" %data2.hex())
                        self.source.send(data2)
                elif ctype== 0x09:
                    serial_num = data[13]*256+data[14]
                    if SERIAL_NUM != serial_num:
                        continue
                    serial_num =  (serial_num + 1)& 0xffff
                    if self.config:
                        if self.last_com == 'get_switch_num':
                            print('get_switch_num')
                            data2 = data[15:data[2]-1]   
                            print('data=%s' %data2.hex())
                            self.cp.set('SWITCH','num',str(data2[0]*256+data2[1]))
                            self.last_com = 'get_switch'
                            DEVICE_NUM = 0
                            SERIAL_NUM = serial_num
                            print('begin get switch,SERIAL_NUM=%d'%SERIAL_NUM)
                            self.send_ack(b'\x09\x4d\x92',SERIAL_NUM,DEVICE_NUM)
                        elif self.last_com == 'get_switch':
                            print('get_switch')
                            data2 = data[15:data[2]-1]   
                            print('data=%s' %data2.hex())
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
                            print('get_switch_area_num')
                            data2 = data[15:data[2]-1]   
                            print('data=%s' %data2.hex())
                            self.cp.set('AREA','num',str(data2[0]*256+data2[1]))
                            self.last_com = 'get_switch_area'
                            DEVICE_NUM = 0
                            SERIAL_NUM = serial_num
                            print('begin get switch,SERIAL_NUM=%d'%SERIAL_NUM)
                            self.send_ack(b'\x0a\x4c\x90',SERIAL_NUM,DEVICE_NUM)
                        elif self.last_com == 'get_switch_area':
                            print('get_switch_area')
                            data2 = data[15:data[2]-1]   
                            print('data=%s' %data2.hex())  
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
                                self.cp.write(open('s2mqtt.ini','w'))
                                print("write configuration file finish")
                                self.config = False
                            else:    
                                self.send_ack(b'\x0a\x4c\x90',SERIAL_NUM,DEVICE_NUM)
                    elif dtype == '1046':
                        if self.config:
                            print("config....,exit")
           #                 continue
#                       print("SERIAL_NUM=%d,data[13-14]=%d" %(SERIAL_NUM,data[13]*256+data[14]))
                        self.reply_num = 0x29
                        data2= bytearray.fromhex("aa00094da1166900e0")
                        data2[5:7] = serial_num.to_bytes(length=2,byteorder='big')
                        data2[7] = DEVICE_NUM
                        data2[8]=(0x035f-data2[5]-data2[6]-data2[7])&0xff
                        self.reply_num = self.reply_num - 1
                        SERIAL_NUM = serial_num
                        print("Send get Next:%s" %data2.hex())
                        self.source.send(data2)
                    elif dtype == '1442':
                        serial_num =  data[13]*256+data[14]
                        data_a = {}
                        data_a[data[15:17].hex()] = data[17:19].hex()                 
                        print(data_a)
                        self.mqtt.send("switch2mqtt/permay/config/local", str(data_a))
                    elif dtype == '2c2a':
                        serial_num =  data[13]*256+data[14] 
                        data2 = data[15:data[2]-1]
                        print("data=%s" %data2.hex())
                        data_a = {}
                        for ii in range(len(data2)):
                            for yy in range(4):
                                ss = (data2[ii] >> (6-yy*2))& 0x3
                                if ss >1:
                                    data_a[self.cp['LOCAL']["%04x"%(ii*4+yy)]]='11'
                                elif ss ==1:    
                                    data_a[self.cp['LOCAL']["%04x"%(ii*4+yy)]]='01'
                        str2 = "%04x"%DEVICE_NUM + str(data_a)          
                        print(str2)
                        self.mqtt.send("switch2mqtt/permay/config/area", str2)        
                    else:
                        self.data = data[15:data[2]-1]   
                        self.refresh(dtype)
       #         if dtype == '094da1':
       #             SERIAL_NUM = data[5]*256 + data[6]
       #             DEVICE_NUM = data[7]
            except Exception as ex:
                log("redirect error:"+str(ex))

class mqtt2(threading.Thread):
    def __init__(self,host,port,user,password,sock):
        threading.Thread.__init__(self)
        self.host = host 
        self.port = port
        self.sock = sock
        self.client = mqtt.Client()
        self.client.username_pw_set(user,password)
        self.client.on_message = self.on_message_come # 消息到来处理函数
        self.client.on_connect = self.on_connect
        log("New Pipe create:%s->%s" % (self.sock.getpeername()))
   
    def run(self):
        self.client.connect(self.host, self.port,60)
        self.client.loop_forever()
    
    def on_connect(self, client, userdata, flags, rc):
        self.client.subscribe("switch2mqtt/permay/control/#", 1)
        log("conncet MQTT [%s:%d] with code:%s." %(self.host,self.port,str(rc)))
         
    def send(self, topic, data):
        self.client.publish(topic, data, 0)
        log("Send MQTT:%s." %data)
           
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
                    print('SERIAL_NUM=%x;DEVICE_NUM=%02x;' %(SERIAL_NUM,DEVICE_NUM))
                    self.sock.send(data)
                    log("Redirect MQTT:%s" %data.hex())
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
                           print('SERIAL_NUM=%x;DEVICE_NUM=%02x' %(SERIAL_NUM,DEVICE_NUM))                       
                           self.sock.send(data)                       
                           log("Redirect MQTT:%s" %data.hex())
            except Exception as ex:
                log("Redirect MQTT error:"+str(ex))

class s2mqtt(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        cf=configparser.ConfigParser("")
        cf.read("config.ini")
        self.host=cf.get("SERVER","host")
        self.port=cf.getint("SERVER","port")
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
            mymqtt = mqtt2(self.mhost,self.mport,self.muser,self.mpassword, self.newsock)
            mymqtt.start()
            p1=pipethread(self.newsock,mymqtt)
            p1.start()
            p1.getconf()

if __name__=='__main__':
    mapp = s2mqtt()
    mapp.start()
