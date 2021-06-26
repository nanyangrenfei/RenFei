# -*- coding:utf-8 -*-
import math
import csv,os,chardet,json
import time,re
from pandas import  DataFrame
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED, ALL_COMPLETED,as_completed

target_distance = 1000
target_filepath = r"D:\testfile\SiteFile\SiteList.csv"
base_write_path = r"D:\testfile"
base_original_filepath = r"D:\testfile\GridFile"
local_time = time.strftime('%Y-%m-%d-%H-%M-%S',time.localtime(time.time()))
all_original_file = os.listdir(base_original_filepath)


def disten_lon_lat(lona,lata,lonb,latb):
	a=math.floor(math.sqrt(math.pow((6.37 * 1000000 * math.pi* (lata - latb) / 180),2)+math.pow((6.37 * 1000000 *
      math.cos((latb + lata) / 2 * math.pi / 180) * math.pi  * (lonb - lona) / 180),2)))
	return(a)
def get_file_code(file_name):
    f3 = open(file=file_name, mode='rb')
    data = f3.read()
    f3.close()
    result = chardet.detect(data)
    return result.get("encoding")
def abc():
    with open(file=base_write_path+"\\a.txt", mode='r') as f:
        data = f.read()
        data = str(data).replace("nan","00")
        print(type(data))
        for one in eval(data):
            with open(file=base_write_path+"\\b.txt", mode='a') as f:
                f.write(str(one[0:5])+"\n")



def get_dataframe_from_csv(path):
    #table = pd.read_csv(path, encoding='gbk', header=0, index_col=None)
    encoding = get_file_code(path)
    table = pd.read_csv(path,encoding=encoding,header=0, index_col=None)
    column_headers = list(table.columns.values)
    table_content = list()
    for row in table.itertuples():
        table_content.append(list(row)[1:])
    # with open(base_write_path+"\\a.txt",mode='w') as f:
    #     f.write(str(table_content))
    # print(table_content)
    # print(column_headers)
    return column_headers,table_content

def get_temp_dataframe(headers_big_csv,row_original,headers_small_csv,row_target,distance=0):
    #创建源始表单行的DataFrame，如果表头与每行数据不对齐，抛出异常
    big_row_data = {}
    if len(headers_big_csv) == len(row_original):
        for i in range(len(headers_big_csv)):
            big_row_data[headers_big_csv[i]] = [row_original[i]]
    else:
        raise ValueError("table header counts error")

    big_row_dataframe = DataFrame(big_row_data)
    # 创建目标表单行的DataFrame，如果表头与每行数据不对齐，抛出异常
    small_row_data = {}
    if len(headers_small_csv) == len(row_target):
        for i in range(len(headers_small_csv)):
            small_row_data[str(headers_small_csv[i])+"-target"] = [row_target[i]]
    else:
        raise ValueError("table header counts error")

    small_row_data["distance"] = [distance]
    small_row_datafrom = DataFrame(small_row_data)
    #合并两个DataFrame
    merge_dateframe = big_row_dataframe.join(small_row_datafrom)
    return merge_dateframe

def write_date_into_csv(**kwargs):
    base_path = kwargs.get("base_write_path")
    dataframe  = kwargs.get("result_dataframe")
    csv_name = kwargs.get("csv_name")
    csv_pre = str(csv_name).split(".")[0]
    #合成写文件绝对路径
    file_name = base_path + "\\" + csv_pre + "_result"  + local_time + ".csv"
    dataframe.to_csv(file_name, index=None,mode='a',encoding="utf_8_sig")

def recycle_calculation(headers_big_csv,rows_big_csv,headers_small_csv,rows_small_csv,csv = None):
    result_dataframe  = pd.DataFrame()
    for row_original in rows_big_csv:
        for row_target in rows_small_csv:
            try:
                distance = disten_lon_lat(float('%.6f' % float(row_original[0])),float('%.6f' % float(row_original[1])),
                                          float('%.6f' % float(row_target[1])),float('%.6f' % float(row_target[2])))
            except IndexError as e:
                print ("lat and lon are abnalmal,big csv:{},small csv:{}".format(row_original[0:2],row_target[0:2]))
            if distance < target_distance:
                single_dataframe = get_temp_dataframe(headers_big_csv,row_original,headers_small_csv,row_target,distance=distance)
                print("big csv:{},small csv:{},their distance is:{}".format(row_original[0:2],row_target[0:2],distance))
                result_dataframe = result_dataframe.append(single_dataframe)
    return result_dataframe,csv

def main():
    pool = ThreadPoolExecutor(max_workers=len(all_original_file))
    headers_small_csv, rows_small_csv = get_dataframe_from_csv(target_filepath)
    all_task = list()
    for csv in all_original_file:
        print("The position is calculating in {} file".format(csv))
        headers_big_csv, rows_big_csv = get_dataframe_from_csv(base_original_filepath + "\\" + csv)
        task = pool.submit(recycle_calculation,headers_big_csv,rows_big_csv,headers_small_csv,rows_small_csv,csv = csv)
        all_task.append(task)
    for future in as_completed(all_task):
        result_dataframe,csv = future.result()
        if not result_dataframe.empty:
            write_date_into_csv(base_write_path = base_write_path,result_dataframe = result_dataframe ,csv_name = csv)
        else:
            print("All  position  less than set distance {} csv".format(csv))

    wait(all_task, timeout=None, return_when=ALL_COMPLETED)
    pool.shutdown()

if __name__=="__main__":
    # = time.time()
    #recycle_calculation()
    main()
    #abc()
    #get_dataframe_from_csv(r"D:\testfile\GridFile\Area01GridCoverageGeo.csv")
    #print(time.time() - time1)

