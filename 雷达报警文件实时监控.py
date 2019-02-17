import time
import os
from sql_server import *
import linecache
import pickle

init_flag = True  # 初次加载程序
record_count = 0
mobile_num = ['18058062001', '13758052598', '13868214771', '13567652817']


def conn_sql():
    conn_content = DataBase('10.138.5.155', 'sa', 'zs58477', 'Radar')
    conn_sms = DataBase('172.21.154.2', 'sa', 'sa', 'SMSProxy')
    return conn_sms, conn_content


def sql_insert(new_line):
    receive_time = time.strftime('%Y-%m-%d %H:%M:%S')
    pickle_file = open('Alarm_data.pkl', 'rb')
    alarm_data = pickle.load(pickle_file)
    date = new_line.split()[0]
    date = date[:10] + ' ' + date[11:19]
    try:
        code = int(new_line.split()[2])
        state = new_line.split()[1]
        text = " ".join(new_line.split()[3:])
    except:
        code = int(new_line.split()[3])
        state = new_line.split()[1] + ' ' + new_line.split()[2]
        text = " ".join(new_line.split()[4:])
    print(date, state, text)
    sql = "insert into Alarm(Datetime,Code,State,Text) values('%s','%s','%s','%s')" % (date,
                                                                                       code,
                                                                                       state,
                                                                                       text)

    conn_sms, conn_content = conn_sql()
    new_line = '有新的雷达报警：\n' + '时间：' + date + '\n' + '报警码：' + str(code) + '\n' + '报警信息：' + alarm_data.get(code)
    for each_num in range(len(mobile_num)):
        sql_sms = "insert into msgbuff(mobileno,receivetime,msg,pri) values('%s','%s','%s','%s')" % (mobile_num[each_num],
                                                                                                 receive_time,
                                                                                                 new_line,
                                                                                                 '1')
        conn_sms.insert_date(sql_sms)
    conn_content.insert_date(sql)
    return


while True:
    # 2019021617
    init_time_m = time.strftime('%Y%m%d%H', time.localtime())
    # 20190216
    init_time = init_time_m[0:8]
    # 20190216_Alarm.log
    file_name = str(init_time) + '_Alarm.log'
    # Z:\20190216_Alarm.log
    net_name = 'Z:\\' + file_name
    print('start monitoring file ' + net_name + ' ...')
    # 检测文件是否存在
    if not os.path.exists(net_name):
        time.sleep(20)
        continue
    try:
        file_info = os.stat(net_name)
        init_mtime = file_info.st_mtime
        timeArray = time.localtime(init_mtime)
        # 文件存在，获取文件修改时间2019021602
        otherStyleTime = time.strftime("%Y%m%d%H", timeArray)
        # 如果当前时间比文件修改时间按小时比较小于一个小时，报警！
        if int(init_time_m) - int(otherStyleTime) < 1:
            with open(net_name, 'r') as f:
                count = len(f.readlines())
            for i in range(count - record_count):
                # 读取某一行
                new_line = linecache.getline(net_name, count - i)
                sql_insert(new_line)
                # 清缓存
                linecache.clearcache()
            record_count = count
        time.sleep(10)
    except:
        pass
