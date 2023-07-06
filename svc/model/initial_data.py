#폴더에 있는 모든 파일을 전처리 한 후, gcs에 업로드 하는 로직(초기 적재)

import os
import re
import json
import pandas as pd
from google.cloud import storage
from google.resumable_media.requests import ResumableUpload
import os
import logging
from svc.common import upload
from svc.common import utils
from svc.common import env
import google.auth.transport.requests as tr_requests
from google.oauth2 import service_account
from google.cloud.storage.fileio import BlobWriter


# 초기 로깅 셋팅
logger = logging.getLogger("info")
errLogger = logging.getLogger("error")
initial_data_Logger = logging.getLogger("initial_data")

GCS_BUCKET_NAME = str(os.environ.get("GCS_BUCKET_NAME"))
GCS_DESTINATION = f"batch/data/" 
DIR_PATH = 'C:/test4'  

#  초기 적재를 위한 CLASS 
class Initial_Data():
    def __init__(self, type , status):
        self.type = type
        self.status = status
        self.preprocess_data()
        
    # S3 버킷에서 전체 다운로드 한 초기 전체 파일을 불러오는 함수
    def get_files(self): 
        FOLDER_LIST = os.listdir(DIR_PATH)
        
        # DIR_PATH 같은 경우 -> 초기 다운로드 받은 전체 파일이 로컬에 있을 때, 그 파일이 있는 로컬 PATH를 기입
        
        FILE_LIST = []
        for i in range(len(FOLDER_LIST)):
            
            # 20230202 폴더 속에 20230202.CSV 형식의 파일이 존재하기 때문에 FOR 문 실행 
            inside_file = os.listdir(f'{DIR_PATH}/{FOLDER_LIST[i]}')
            
            if "." not in FOLDER_LIST[i] : 
                for file_name in inside_file: 
                    
                    address = f'{DIR_PATH}/{FOLDER_LIST[i]}/{file_name}'
                    
                    FILE_LIST.append(address)
            else: 
                address = f'{DIR_PATH}/{FOLDER_LIST[i]}'
                FILE_LIST.append(address)
                
        i+=1
        return FILE_LIST
    
    # 초기 적재 전체 파일을 하나씩 전처리 하는 함수
    def preprocess_data(self):
        headers = ['DATE','BIRTH_DATE',"REMOVE",'COM_CODE','MEMBER_NO','NAME','COMPANY','INCOME','GENDER']

        # 폴더 내 모든 파일 업로드 할 때
        data_list = self.get_files()
        
        for i, x in enumerate(data_list):
            
            # 파일 읽고 전처리 구간
            org_FilePath = data_list[i]
            load = pd.read_csv(org_FilePath,  low_memory = False)
            data = load.drop(['REMOVE', 'COMPANY'], axis = 'columns')
            print(data)
            
            # 파일명 변경 구간 - custom 구간 
            recentFilename = org_FilePath.split('/')[-1]

            # DATAFRAME을 JSONL로 변경 후 GCS에 초기 적재 
            data = utils.dataframe_to_jsonl(data)
            gcsupload = upload.Upload([])
            destFilepath = f"{GCS_DESTINATION}{recentFilename}"
            gcsupload.fn_string_upload(data, destFilepath, errLogger , self.status)
           
  

