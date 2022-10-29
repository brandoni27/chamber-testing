"""
This script reads the current chamber temperature and stores the data
in CSV and SQLite databases. Timestamps of the current temperature are also recorded.
"""

#!/usr/bin/env python3
import os, sys, subprocess, time, multiprocessing as mp
from subprocess import check_output
from datetime import datetime
from multiprocessing import Queue
import pandas as pd
import numpy as np
import sqlite3

'''

List of the chambers and their corresponding indexes in the arrays. 

corp: 0:ch7 | 1:ch3 | 2:ch6 | 3:ch8 | 4:ch9 | 5:ch10 | 6:ch11 | 7:ch12 | 8:ch13 | 9:ch14 | 10:ch17 | 11:ch21 | 12:ch22
prod: 0:ch1 | 1:ch2 | 2:ch4 | 3:ch5 | 4:ch16 | 5:ch14 | 6:ch15 | 7:ch12 | 8:ch17 | 9:ch18 | 10:ch20 | 11:ch19 |

'''


'''
  Function to create connection to local sqlite3 database 
    create_connection(db_file)
      :param db_file: path to database file  
      :return: Connection object or None
'''  
def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)
    return conn


## Function runs tvac.py script, formats output for only the temperature as a float with 2 decimal places. ## 
## Chamber name, current temperature, and datetime are output and appended to a master list  ##
def temp_check(chamber):
  results = []
  args = ['./tvac.py',str(chamber),'read_temp']
  out = str(check_output(args))
  fout = out.split('temperature')
  value = str(''.join(c for c in fout[1] if (c.isdigit() or c =='.')))
  fvalue = "{:.2f}".format(float(value))
  date = str(datetime.strptime(datetime.now().strftime('%Y-%m-%d %H:%M'), '%Y-%m-%d %H:%M'))
  results = [chamber, fvalue, date]
  final_list.append(results)

  
## A function to print the unfiltered response from the tvac.py script. Used for debugging. ##
def unfiltered_check(chamber):
  args = ['./tvac.py',str(chamber),'read_temp']
  out = str(check_output(args))
  print(out)
  print("Character count: ", len(out))


if __name__=='__main__':
  
  # SQL database connection #
  database = r"/usr/local/google/home/brwashington/Documents/code/database-test.db"
  conn = create_connection(database)

  # Used to record responses from multiprocessed functions and append to global list # 
  manager = mp.Manager()
  final_list = manager.list()
  
  # Lists of Corp and Prod chambers #
  corp_chambers = ['ch7','ch3','ch6','ch8','ch9','ch10','ch11','ch12','ch13','ch14','ch17','ch21','ch22']
  prod_chambers =['ch1','ch2','ch4','ch5','ch16','ch14','ch15','ch12','ch7','ch18','ch20','ch19']
  
  # Creating headers for, and initiating dataframe #
  column_names = ['chamber',' value', 'date']
  df=pd.DataFrame(column_names)
  
  # Using multiprocessing to run the function on the chambers at the same time #
  p1 = mp.Process(target=temp_check, args=(corp_chambers[1],))
  p2 = mp.Process(target=temp_check, args=(corp_chambers[2],))
  p3 = mp.Process(target=temp_check, args=(corp_chambers[3],))
  p4 = mp.Process(target=temp_check, args=(corp_chambers[4],))
  p5 = mp.Process(target=temp_check, args=(corp_chambers[5],))
  p6 = mp.Process(target=temp_check, args=(corp_chambers[8],))
  p7 = mp.Process(target=temp_check, args=(corp_chambers[9],))
  p8 = mp.Process(target=temp_check, args=(corp_chambers[10],))
  p9 = mp.Process(target=temp_check, args=(corp_chambers[11],))
  p10 = mp.Process(target=temp_check, args=(corp_chambers[12],))

  # Initiating multiprocessing for each function #
  p1.start()
  p2.start()
  p3.start()
  p4.start()
  p5.start()
  p6.start()
  p7.start()
  p8.start()
  p9.start()
  p10.start()

  # Ending multiprocessing functions once complete #
  p1.join()
  p2.join()
  p3.join()
  p4.join()
  p5.join()
  p6.join()
  p7.join()
  p8.join()
  p9.join()
  p10.join()
  
  # Initiating lists to transfer data from final_list in preparation for dataframe #
  chamber_out = []
  temp_out = []
  date_out = []
  time_out = []

  # Loops to transfer data from final_list to empty lists #
  for i in range(0,len(final_list)):
    chamber_out.append(final_list[i][0])
  for i in range(0,len(final_list)):
    temp_out.append(final_list[i][1])

  # Loops through datetime column and splits datetime into date_out and time_out lists separately #  
  for i in range(0,len(final_list)):
    text_1 = final_list[i][2]
    text_2 = text_1.split()
    date_out.append(text_2[0])
    time_out.append(text_2[1])

  # Dumping final lists into dataframe #
  df_out = pd.DataFrame({
    'chamber_name': chamber_out,
    'temperature': temp_out,
    'time_stamp': time_out,
    'date_stamp': date_out
    })
  
  # Outputs Dataframe to both CSV file and SQLite database # 
  df_out.to_csv("Chamber_data.csv", mode='a', index=False, header=False)
  df_out.to_sql('chamber_temperatures', con=conn, if_exists='append', index=False)
  



'''

This is what the unfiltered output from tvac.py looks like

b'LOAS2 expires in 18h 31m\n
corp/normal expires in 18h 31m\n
172.23.151.123 is up! Chamber is corp\n
GCERT is Good To Go\n
logging in to pi\n
\nThe current chamber temperature is:
\n
\n44.99
\ndegrees C
\n'

'''

