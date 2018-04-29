
import numpy as np
# import matplotlib.pyplot as plt


import datetime
import math
import MySQLdb
import Queue
import socket
import sys
import threading
#import timecmd
import collections



import numpy
#import numpy.random
#import matplotlib.pyplot as plt
#import shapely.geometry as sg


from collections import defaultdict
from random import randint


from multiprocessing import Process
import multiprocessing.reduction

#def update_line(num, data, line):
#    line.set_data(data[..., :num])
#    return line,
#x = [1, 2, 3, 4, 5, 6, 7, 8, 9]
#y = [1, 9, 7, 3, 5, 6, 8, 4, 2]
#plt.plot(x, y)

NODELIST = {'Occupancy-Li01', 'Occupancy-Li02', 'Occupancy-Li03'}

b185 = [datetime.datetime(2017, 7, 18, 11, 36, 00), datetime.datetime(2017, 7, 18, 11, 48, 00)]
b145 = [datetime.datetime(2017, 7, 18, 11, 49, 00), datetime.datetime(2017, 7, 18, 11, 53, 00)]
b142 = [datetime.datetime(2017, 7, 18, 11, 54, 00), datetime.datetime(2017, 7, 18, 11, 58, 00)]
b140 = [datetime.datetime(2017, 7, 18, 11, 59, 00), datetime.datetime(2017, 7, 18, 12, 04, 00)]
b192 = [datetime.datetime(2017, 7, 18, 13, 36, 00), datetime.datetime(2017, 7, 18, 13, 40, 00)]
b190 = [datetime.datetime(2017, 7, 18, 13, 41, 00), datetime.datetime(2017, 7, 18, 13, 44, 00)]
b158 = [datetime.datetime(2017, 7, 18, 13, 46, 00), datetime.datetime(2017, 7, 18, 13, 50, 00)]
b157 = [datetime.datetime(2017, 7, 18, 13, 52, 00), datetime.datetime(2017, 7, 18, 13, 55, 00)]
b217 = [datetime.datetime(2017, 7, 18, 14, 01, 00), datetime.datetime(2017, 7, 18, 14, 03, 00)]
b288 = [datetime.datetime(2017, 7, 18, 14, 06, 00), datetime.datetime(2017, 7, 18, 14, 10, 00)]
b239 = [datetime.datetime(2017, 7, 18, 14, 15, 00), datetime.datetime(2017, 7, 18, 14, 18, 00)]
b236 = [datetime.datetime(2017, 7, 18, 14, 19, 00), datetime.datetime(2017, 7, 18, 14, 22, 00)]
b241 = [datetime.datetime(2017, 7, 18, 14, 27, 00), datetime.datetime(2017, 7, 18, 14, 30, 00)]
b242 = [datetime.datetime(2017, 7, 18, 14, 31, 00), datetime.datetime(2017, 7, 18, 14, 34, 00)]

t_b251 = [datetime.datetime(2017, 9, 14, 17, 41, 00), datetime.datetime(2017, 9, 14, 17, 45, 00)]
t_b242 = [datetime.datetime(2017, 9, 14, 17, 47, 00), datetime.datetime(2017, 9, 14, 17, 49, 00)]
t_b236 = [datetime.datetime(2017, 9, 14, 17, 50, 00), datetime.datetime(2017, 9, 14, 17, 52, 00)]

#Nick        = [datetime.datetime(2017, 7, 18, 11, 36, 00), datetime.datetime(2017, 7, 18, 11, 48, 00)]
b245_hall   = [datetime.datetime(2017, 7, 18, 14, 24, 00), datetime.datetime(2017, 7, 18, 14, 26, 00)]
#b236_hall   = [datetime.datetime(2017, 7, 18, 14, 11, 00), datetime.datetime(2017, 7, 18, 14, 13, 00)]
Elev        = [datetime.datetime(2017, 7, 18, 14, 36, 00), datetime.datetime(2017, 7, 18, 14, 40, 00)]

v_b236 = []
vt_b236 = []
v_b288 = [list(), list(), list()]
v_b241 = [list(), list(), list()]

Cal_List = list()

class SQL_Database():

    db = MySQLdb
    cursor = MySQLdb
    db_close = False

    # Initialization function
    def __init__(self, db_host, db_port, db_user, db_passwd, db_database):
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_passwd = db_passwd
        self.db_database = db_database
        return

    # Connects to the SQL DB specified in the initialization.
    # if the connection fails print out the error message.
    def db_connect(self):
        try:
            # Open database connection
            self.db = MySQLdb.connect(host = self.db_host, port = self.db_port, user = self.db_user, passwd = self.db_passwd, db = self.db_database)

            self.cursor = self.db.cursor()                       # Prepare a cursor object using cursor() method
            self.cursor.execute("SELECT VERSION()")              # Execute SQL query using execute() method.
            db_version = self.cursor.fetchone()[0]               # Fetch a single row using fetchone() method as a tuple.


            status = '---- Connected to SQL DB ----\n\tHost: {}\n\tPort: {}\n\tDatabase: {}\n\tDatabase version : {}\n\t'.format(self.db_host, self.db_port, self.db_database, db_version)
        except:
            status = "ERROR - SQL DB: connection to SQLDB failed.\n\t"
            for index, msg in enumerate(sys.exc_info()):
                status += "SYS ERROR {}: {}\n\t".format(index, msg)

        print status

    # This function returns the entire database as an array.
    # Each array element contains 1 entry from the database.
    # Currently this function is only being used for debugging purposes (3-5-16)
    def db_read(self, cmd):
        try:
            self.cursor.execute(cmd)
            db_data = self.cursor.fetchall()
        except:
            db_data = 'error'

        return db_data

    def run(self, data_queue):

        self.db_connect()
        threading._sleep(1)

        while not self.db_close:
            threading._sleep(5)

        self.db.close()
        print 'SQL DB: CLOSED'

class Data_Processing():

    window_Start_date = datetime.date(2017, 5, 23)
    window_Start_time = datetime.time(12, 30, 00)
    window_Start = datetime.datetime.combine(window_Start_date, window_Start_time)

    window_End_data = datetime.date(2017, 5, 23)
    window_End_time = datetime.time(12, 31, 00)
    window_End = datetime.datetime.combine(window_End_data, window_End_time)

    def __init__(self, sql):
        db = sql
        return

    def run(self):
        self.calibrate()
        while True:
            threading._sleep(5)

        return

    def get_clients_within_boarders(self, w_Start, w_End):

        # SQL CMD get data points within frame window
        sql_cmd = """SELECT * FROM scapy_drli WHERE date_time between '{}' and '{}';""".format(
            w_Start.strftime('%Y-%m-%d %H:%M:%S'),
            w_End.strftime('%Y-%m-%d %H:%M:%S')
        )

        sql_data = sql.db_read(sql_cmd)


        return

    def calibrate(self):

        t_mac = 'MAC_Address'

        v_template = [list(), list(), list()]
        building = [["b236", b236], ["b239", b239], ["b241", b241], ["b288", b288], ["b242", b242]]


        for room in building:

            v_caldata = []

            t_begin = room[1][0]
            t_end = room[1][1]

            # SQL CMD get data points within frame window
            sql_cmd = """SELECT * FROM scapy_drli WHERE mac = '{}' and date_time between '{}' and '{}';""".format(
                t_mac,
                t_begin.strftime('%Y-%m-%d %H:%M:%S'),
                t_end.strftime('%Y-%m-%d %H:%M:%S')
            )
            v_temp = sql.db_read(sql_cmd)

            # print "start: {} end: {} Duration: {}".format(t_begin, t_end, t_end-t_begin)
            # print "seconds: {}".format((t_end-t_begin).seconds/20)

            temp = [list(), list(), list()]

            for time_segment in range(0, int((t_end-t_begin).seconds/20)):
                windowS = t_begin + datetime.timedelta(0,20*(time_segment))
                windowE = t_begin + datetime.timedelta(0,20*(time_segment+1))
                #print "start: {} | end: {}".format(windowS, windowE)

                temp = [list(), list(), list()]

                for pr in v_temp:
                    if windowS < pr[2] and pr[2] < windowE:

                        if pr[3] == "Occupancy-Li01":
                            # print probe_request
                            temp[0].append(pr[1])
                        if pr[3] == "Occupancy-Li02":
                            # print probe_request
                            temp[1].append(pr[1])
                        if pr[3] == "Occupancy-Li03":
                            # print probe_request
                            temp[2].append(pr[1])

                if temp[0] != [] and temp[1] != [] and temp[2] != []:
                    v_caldata.append([np.average(temp[0]), np.average(temp[1]), np.average(temp[2])])

                #print " temp: {} ".format(temp)
                # temp1_avg = numpy.average(temp1)
                # print temp1
                # print temp1_avg

            #print "vector b236: {}".format(v_b236)
            #print "vector b288: {}".format(v_b288)
            #print "vector b241: {}".format(v_b241)

            Cal_List.append([room[0], v_caldata])

        print "Calibration Matrix\n-------------------------------------"
        for l in Cal_List:
            print "{}: {}".format(l[0], l[1])
        print ""
        print ""

        self.KNN()

        return


    def KNN(self):
        t_mac = {'10:A5:D0:30:19:98'}
        t_begin = t_b242[0]
        t_end = t_b242[1]

        for mac in t_mac:

            # SQL CMD get data points within frame window
            sql_cmd = """SELECT * FROM scapy_drli WHERE mac = '{}' and date_time between '{}' and '{}';""".format(
                mac,
                t_begin.strftime('%Y-%m-%d %H:%M:%S'),
                t_end.strftime('%Y-%m-%d %H:%M:%S')
            )
            v_temp = sql.db_read(sql_cmd)

            distance_from_Cal = []

            t_temp = [list(), list(), list()]

            for time_segment in range(0, int((t_end - t_begin).seconds / 20)):
                windowS = t_begin + datetime.timedelta(0, 20 * (time_segment))
                windowE = t_begin + datetime.timedelta(0, 20 * (time_segment + 1))

                temp = [list(), list(), list()]

                for pr in v_temp:
                    if windowS < pr[2] and pr[2] < windowE:

                        if pr[3] == "Occupancy-Li01":
                            # print probe_request
                            t_temp[0].append(pr[1])
                        if pr[3] == "Occupancy-Li02":
                            # print probe_request
                            t_temp[1].append(pr[1])
                        if pr[3] == "Occupancy-Li03":
                            # print probe_request
                            t_temp[2].append(pr[1])

                if t_temp[0] != [] and t_temp[1] != [] and t_temp[2] != []:
                    vt_b236.append([windowS, [np.average(t_temp[0]), np.average(t_temp[1]), np.average(t_temp[2])]])

            # Euclidean distance matrix: sqrt( sum from node 0 to node n of (Calibration_data - Real_Time_Data)^2 )
            for ll in Cal_List:
                distance_from_Cal.append(["", list()])
                for data_point in vt_b236:
                    DFC_Index = len(distance_from_Cal)-1
                    distance_from_Cal[DFC_Index][0] = ll[0]
                    for cal_data_point in ll[1]:
                            distance_from_Cal[DFC_Index][1].append(

                                    math.sqrt(
                                        math.pow((cal_data_point[0] - data_point[1][0]), 2) +
                                        math.pow((cal_data_point[1] - data_point[1][1]), 2) +
                                        math.pow((cal_data_point[2] - data_point[1][2]), 2)
                                    )

                            )

            print "Distance Matrix\n-------------------------------------"
            for dList in distance_from_Cal:
                print "{}: {}".format(dList[0], dList[1])
            print ""

        temp_avg = 200

        print "Averages for Distance Matrix\n-------------------------------------"
        for dList in distance_from_Cal:

            if numpy.mean(dList[1]) < temp_avg:
                temp_avg = dList[0]

        print temp_avg
        print ""

        return




if __name__ == '__main__':

    sql_queue       = Queue.Queue()

    workerThreads = []

    db_host = 'server_address'
    db_port = 3306
    db_user = 'user'
    db_passwd = 'password'
    db_database = 'database'


    sql             = SQL_Database(db_host, db_port, db_user, db_passwd, db_database)
    data_proc       = Data_Processing(sql)
    # mpl_handeler    = MatPlotLib_Handeler()

    sql.db_table    = 'table'

    t_sql           = threading.Thread(target=sql.run, args=(sql_queue,))
    t_dpr           = threading.Thread(target=data_proc.run, args=())
    # t_mpl           = threading.Thread(target=mpl_handeler.run, args=())

    workerThreads.append(t_sql)
    workerThreads.append(t_dpr)
    #workerThreads.append(t_mpl)

    for t in workerThreads:
        t.start()
        threading._sleep(6)
        print "THREAD: {} thread started.".format(t.name)
