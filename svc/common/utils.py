import os
import shutil
import json
import platform
import socket
import datetime
import re
import time
import pandas as pd
import glob

ALLOWED_EXTENSIONS = [".csv" , ".xlsx"]
EQULAS_CHECK_DIRECTORY = "datas/excel"

def netchecker() : 
    ipaddress=socket.gethostbyname(socket.gethostname())
    if ipaddress=="127.0.0.1":
        print("You are not connected to the internet!")
    else:
        print("You are connected to the internet with the IP address of "+ ipaddress )


# directory 만드는 함수
def fnmkdir(dirname, errorMsg):
    try:
        if not os.path.exists(dirname):
            os.makedirs(dirname)
    except OSError:
        print("Directory Create Error : ", errorMsg)

# 디렉토리 및 하위에 포함된 파일 삭제
def fndel_dir_with_file(dirpath, errorMsg) : 
    try : 
        if os.path.exists(dirpath):
            shutil.rmtree(dirpath, ignore_errors=False)
    except OSError:
        print("DirWithFile Delete Error : ", errorMsg) 

# 특정 파일을 삭제 
def fndel_file(filepath , errorMsg) : 
    
    os_name = platform.system()
    # if(os_name == 'Linux') :
        
    if (os_name == 'Windows') : 
        filepath = filepath.replace("/" , "\\")

    try : 
        if os.path.exists(filepath):
            os.remove(filepath)
    except OSError as e:
        print("File Delete Error : ", errorMsg)  
        print(f"Error origin : {e}")

def get_os_name(): 
    return platform.system()

def get_contains(str , search_word):
    if search_word in str:
        return True
    else:
        return False

# Dataframe 형태의 pandas data를 JSONL 형태로 변경
def dataframe_to_jsonl(df) : 
    result = df.to_json(orient="records")
    parsed = json.loads(result)
    jsonlines_data = ""
    for index in range(len(parsed)) :
        try:
            row = parsed[index]
    
            temp_data = json.dumps(row, ensure_ascii=False).encode('utf8')

            jsonlines_data += temp_data.decode() + "\n"

        except Exception as e:
            print(e)
    return jsonlines_data


# GCS String 업로드 후 정합성 확인하는 함수
# 정합성 확인 후 전달 받은 Logger 로 로그 남김 (Logger 별 다른 파일로 log 기록)
def fn_check_contents_in_gcs(bucket, org_Contents_Rows, blobName , typeLogger):
    # GCS 업로드 파일 객체 가져오기
    blob = bucket.get_blob(blobName)
    # Count rows
    blobRowCount = len(blob.download_as_string().splitlines())

    typeLogger.info(org_Contents_Rows)
    typeLogger.info(blobRowCount)

    return org_Contents_Rows == blobRowCount

def fn_check_contents_in_gcs_jsonList(bucket , dest_path ,  org_info , typeLogger , getType):
    OrgCount = len(org_info)
    SuccessCount = 0
    for rows in org_info : 
        tableName = rows['table_name']

        blobSize = 0
        blobRowCount = 0
       
        blob = bucket.get_blob(dest_path + "/" + tableName)
       
        try : 
            if getType == 'size' :
                table_size = rows['table_size']
                blobSize = blob.size
                typeLogger.info(f"tableName : {tableName} ::: table_size : {table_size} ::: dest_size : {blobSize} ::: result : {int(table_size) == blobSize}"  )
                if int(table_size) == blobSize : 
                    SuccessCount += 1

            elif getType == 'rowcnt':  
                table_rowcnt = rows['table_rowcnt']      
                blobRowCount = len(blob.download_as_string().splitlines())
                typeLogger.info(f"tableName : {tableName} ::: table_rowcnt : {table_rowcnt} ::: dest_rowcnt : {blobRowCount} ::: result : {table_rowcnt == blobRowCount}"   )
        except Exception as e:
            typeLogger.info(f"tableName : {tableName} ::: Error Rise"   )
            print(e)
        
    typeLogger.info(f"OrgTable Count : {OrgCount} ::: SuccessCount : {SuccessCount}"   )
    


# GCS csv 파일 업로드 후 정합성 확인하는 함수
# 정합성 확인 후 전달 받은 Logger 로 로그 남김 (Logger 별 다른 파일로 log 기록)
# def fn_check_csv_in_gcs(bucket, org_Contents_Rows, _fileName , typeLogger):
#     # GCS 업로드 파일 객체 가져오기
#     blob = bucket.get_blob(blobName)

#     # Count rows
#     blobRowCount = len(blob.download_as_string().splitlines())

#     # file의 row count 측정
#     def _fnmakegen(reader):
#         while True:
#             b = reader(2 ** 16)
#             if not b:
#                 break
#             yield b

#     fileRowCount = 0
#     with open(_fileName, "rb") as f:
#         fileRowCount = sum(buf.count(b"\n")
#                             for buf in _fnmakegen(f.raw.read))
    
#     typeLogger.info(org_Contents_Rows)
#     typeLogger.info(blobRowCount)
    
#     return org_Contents_Rows == fileRowCount


