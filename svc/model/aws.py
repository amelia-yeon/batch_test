#  # S3 to vm
# import os
# import boto3
# import pandas as pd
# import logging
# import io
# from svc.common import env
# from svc.common import upload
# from svc.common import utils



# #초기 상수 및 로깅 
# logger = logging.getLogger("info")
# errLogger = logging.getLogger("error")
# awsLogger = logging.getLogger("aws")
# AWS_ACCESSKEY = str(os.environ.get('AWS_ACCESSKEY'))
# AWS_SECRETKEY = str(os.environ.get('AWS_SECRETKEY'))
# AWS_REGION = str(os.environ.get('AWS_REGION'))
# AWS_BUCKETNAME = str(os.environ.get('AWS_BUCKETNAME'))
# AWS_PREFIX = str(os.environ.get('AWS_PREFIX'))
# GCS_DESTINATION = f"batch/daily_data/" 


# #s3 클라이언트 생성
# s3 = boto3.client('s3', 
#                   aws_access_key_id = AWS_ACCESSKEY, 
#                   aws_secret_access_key = AWS_SECRETKEY, 
#                   region_name = AWS_REGION) 

# # AWS에 있는 데이터를 가져오는 class 
# class AWS_DATA():
#     def __init__(self,type , status):
#         self.type = type
#         self.preprocess_data(status)

#     # AWS 버킷 내 가장 하위에 있는 폴더에 가장 최근 파일을 불러오는 함수 
#     def get_recent_file(self):
#         object_list = s3.list_objects(Bucket = AWS_BUCKETNAME, Prefix = AWS_PREFIX)
#         object_list = object_list['Contents']
#         FILE_LIST = []
#         for file in object_list:
#             key = file['Key']
#             FILE_LIST.append(key)
#         FILE_LIST.sort(reverse=True)
        
#         if(len(FILE_LIST) > 0):
#             print(FILE_LIST[0])
#             return FILE_LIST[0]
        
#     # 데이터 컬럼 전처리 함수
#     def preprocess_data(self, status):
#         headers = ['DATE','BIRTH_DATE',"REMOVE",'COM_CODE','MEMBER_NO','NAME','COMPANY','INCOME','GENDER']
        
#         recent_file = self.get_recent_file() 
        
#         # 버킷에 저장된 파일명과 일치하도록 네이밍 바꾸는 과정 
#         recentFilename = recent_file.split('/')[-1]
        
        
#         #버킷 안에 파일 불러오기
#         file_object = s3.get_object(Bucket=AWS_BUCKETNAME, Key=recent_file)
#         file = pd.read_csv(io.BytesIO(file_object['Body'].read()) , header=None)
        
#         file.columns = headers
#         file = file.drop(['REMOVE','COMPANY'], axis = 'columns')
        
#         # DATAFRAME 에서 JSONL로 바뀌는 함수를 이용하여 GCS에 업로드 
#         aws_data = utils.dataframe_to_jsonl(file)
#         gcsUpload = upload.Upload([])
        
#         # GCS 파일 업로드 경로 정리 
#         destFilepath = f"{GCS_DESTINATION}{recentFilename}"
#         gcsUpload.fn_string_upload(aws_data , destFilepath , awsLogger , status)
    


    












