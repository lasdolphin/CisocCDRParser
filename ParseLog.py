# /usr/local/bin/python3.3
# -*- coding: utf-8 -*-
import os, time, datetime
#import pypyodbc
import logging, logging.handlers

#### Настройки ###########
# путь к логам Cisco
path = '/home/shared/'
# файл позиции
pos_file='/usr/local/scripts/pos'
# префикс имени лога и соответствующее ему название для заббикса
cisco_devices={
    'cdrm9':'ciscom9cdr',
    'cdrdl':'ciscoDLcdr',
}
# файлы словарей
terminations_file='/usr/local/scripts/terminations_file'
trunks_file='/usr/local/scripts/trunks'
# адрес заббикса
zabbixIP='194.1.214.23'

# настройка записи в лог
lgfmt = '%(levelname)-8s [%(asctime)s] %(message)s'
logging.basicConfig(format = lgfmt, level = logging.DEBUG) #,filename = u'mylog.log') #NOTSET,DEBUG,ERROR
logger = logging.getLogger()
# настройка e-mail
"""
smtp_handler = logging.handlers.SMTPHandler(mailhost=("exchange2.corp.wildberries.ru", 25),
                                            fromaddr="autoaddusers@wildberries.ru",
                                            toaddrs="admin@wildberries.ru",
                                            subject=u"Cisco log parser error!")
logger.addHandler(smtp_handler)
"""
# константы - позиции в файле
calling=20
called=22
cause=12
ctype=13
positions={}

### Утилиты ############
def read_dict_from_file(file):
    # чтение файла в словарь
    logging.debug("Чтение словаря "+file+" -----------------------")
    d={}
    try:
        with open(file, 'r') as f:
            for line in f:
                line=line.strip()
                if line!="":
                    logging.debug(line)
                    line=line.split("==>")
                    if len(line)==1:
                        d.update({line[0]:0})
                    else:
                        d.update({line[0]:line[1]})
            f.close()
    except:
        logging.debug("Словарь не найден")
    logging.debug("-----------------------")
    return d

def write_dict_to_file(file,dct):
    # запись словаря в файл
    logging.debug("Запись словаря "+file+" -----------------------")
    f=open(file,'w')
    for key in dct:
        line=key+"==>"+str(dct[key])
        logging.debug(line)
        f.write(line+'\n')
    logging.debug("-----------------------")
    f.close()

def zabbix_send(d,serv):
    logging.debug("Send to Zabbix -------------------------------------------")
    for key in d:
        arr=key.split(' ')
        if len(arr)>1:
            b=serv+'.'+arr[len(arr)-1][1:-1]
        else:
            b=serv+'.'+key.replace('+','p')

        logging.debug(str(b)+' '+str(d[key]))
        logger = logging.getLogger()
        if logger.level!=logging.NOTSET:
            os.spawnlp(os.P_WAIT, 'zabbix_sender', 'zabbix_sender','-z', zabbixIP, '-s',str(serv), '-k',str(b), '-o',str(d[key]), '-vv')
    logging.debug("-------------------------------------------")

########################

class CiscoParser:
    # счетчики -------------------
    terminations = {}
    trunks={}
    incount=0
    outcount=0
    ocount=0
    acount=0

    def __init__(self):
        try:
            self.terminations=read_dict_from_file(terminations_file)
            self.trunks=read_dict_from_file(trunks_file)
        except  Exception as e:
            logger.critical(str(e))

    def parse_file(self,file_name,pos=0):
        logging.debug("Reading file: "+file_name+" ------------------------------------------")
        n = 0
        f = open(file_name,'r')
        if pos>0: f.seek(pos) # если нужно, поменять позицию
        list = f.readlines()
        f=f.tell() # запомнить позицию, на которой остановились
        for line in list:
            # разбить строку
            s=line.strip().split(',')
            for i in range(len(s)):
                n+=1
                tmp=s[i].strip()
                if tmp!='':
                    if tmp[0]=='"':
                        tmp=tmp[1:len(tmp)-1]
                    else:
                        try:
                            tmp=int(tmp)
                        except:
                            pass
                else:
                    tmp=None
                s[i]=tmp
            if len(s)==35:
                self.parse_line(s)
            else:
                logging.warning("№ "+str(n)+" "+line)
        logging.debug("------------------------------------------")
        return f

    def parse_line(self,arr):
        callvector=False
        if arr[ctype]=='originate':
            self.ocount+=1
            # подсчет по транкам --------------
            if arr[calling] in self.trunks.keys():
                self.outcount+=1
                if len(arr[calling])<=12:
                    self.trunks[arr[calling]]+=1
            elif arr[called] in self.trunks.keys():
                self.incount+=1
                if len(arr[called])<=12:
                    self.trunks[arr[called]]+=1
            # подсчет по причинам -------------
            if arr[cause] in self.terminations.keys():
                self.terminations[arr[cause]]+=1
            else:
                logging.debug("Write cause: "+arr[cause])
                self.terminations.update({arr[cause]:0})
                self.terminations[a[cause]]+=1
                # сбор причин в файл
                try:
                    with open(terminations_file, 'a') as fw:
                        fw.write(arr[cause])
                        fw.write('\n')
                        fw.close()
                except Exception as e:
                    logger.error(str(e))
        elif arr[ctype]=='answer':
            self.acount+=1

##########################
def parse_file(f,gw_prefix):
    serv=cisco_devices[gw_prefix]
    fn=os.path.join(path,f)
    x = CiscoParser()
    try:
        p=positions[gw_prefix]
        p=p.split(':')
        p=int(p[0])
    except:
        p=0

    p=x.parse_file(fn,p)

    positions[gw_prefix] = str(p)+':'+f
    write_dict_to_file(pos_file,positions)

    zabbix_send(x.trunks,serv)
    zabbix_send(x.terminations,serv)
    logger.debug('======================================================')
    logger.debug('Originate: %i' % x.ocount)
    logger.debug('Answer: %i' % x.acount)
    logger.debug('Outbound calls: %i' % x.outcount)
    logger.debug('Inbound calls: %i' % x.incount)

def get_logfile(gw_prefix):
    files = os.listdir(path)
    current_time=time.time()
    min_file_time=current_time
    file_path = ''
    for f in files:
        file_name=f.split('.')
        if file_name[0]==gw_prefix:
            file_time=time.mktime(datetime.datetime.strptime(file_name[2], "%m_%d_%Y_%H_%M_%S").timetuple())
            delta_time=current_time-file_time
            if delta_time<min_file_time:
                min_file_time=delta_time
                file_path=f
    try:
        p=positions[gw_prefix]
        p=p.split(':')
        if len(p)==2 and p[1]!=file_path:
            positions[gw_prefix]='0:'+file_path
    except:
        pass
    return file_path

### Основной цикл #####################
#files = os.listdir(path)
#for f in files:
#    parse_file(f)
positions = read_dict_from_file(pos_file)
for key,value in cisco_devices.items():
    f=get_logfile(key)
    if f!='':
        parse_file(f,key)
    print(f)