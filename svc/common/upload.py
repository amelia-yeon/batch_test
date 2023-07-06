import time
from google.cloud import storage
from google.resumable_media.requests import ResumableUpload
import glob
import os
import logging
import sys
from tqdm import tqdm
# from svc.common import env
import google.auth.transport.requests as tr_requests
from google.oauth2 import service_account
from google.cloud.storage.fileio import BlobWriter
from svc.common import utils
from time import sleep
import traceback

# 초기 로깅 셋팅
logger = logging.getLogger("info")
errLogger = logging.getLogger("error")

# 상수들
# SAVED_DIRECTORY = env.Env().get('DEFAULT_SAVED_DIRECTORY')
# MAIL_SAVED_DIRECTORY = env.Env().get('MAIL_SAVED_DIRECTORY')
# GC_AUTH_MAIL = env.Env().get('GC_AUTH_MAIL')

GCS_PROJECT_ID = str(os.environ.get("GCS_PROJECT_ID"))
GCS_BUCKET_NAME = str(os.environ.get("GCS_BUCKET_NAME"))
GCS_KEY_FILE = str(os.environ.get("GCS_KEY_FILE"))



ALLOWED_EXTENSIONS = [".xlsx", ".csv"]
UPLOAD_ERROR_FILE_NAMES = []
CHUNK_SIZE = 262144  # This needs to be a multiple of 262144


# SAVED_DIRECTORY에 있는 파일들 GCS에 업로드하는 클래스
class Upload():
    errFiles = []
    chunkSize = CHUNK_SIZE
    logType = ""
    
    def __init__(self, errFiles):
        self.chunkSize = CHUNK_SIZE
        self.errFiles = errFiles
        self.logType = ""
        self.upload_cnt = 0
        print(self.logType)

    # GCS String 업로드 후 정합성 확인하는 함수
    # 정합성 확인 후 전달 받은 Logger 로 로그 남김 (Logger 별 다른 파일로 log 기록)
    def fncheck_contents_in_gcs(self, bucket, org_Contents_Rows, blobName , typeLogger):
        # GCS 업로드 파일 객체 가져오기
        blob = bucket.get_blob(blobName)
        # Count rows
        blobRowCount = len(blob.download_as_string().splitlines())

        print(org_Contents_Rows)
        print(blobRowCount)

        return org_Contents_Rows == blobRowCount

    # GCS 파일 업로드 정합성 확인하는 함수
    
    def fn_check_file_in_gcs(self, bucket, fileName, blobName, type):
        rtn = False

        # GCS 업로드 파일 객체 가져오기
        blob = bucket.get_blob(blobName)

        # 원본 파일과 GCS 파일의 byte size 비교
        def fn_check_file_size(_fileName):
            _fileSize = os.path.getsize(_fileName)
            _blobSize = blob.size
            logger.info(
                f"original file size ({_fileSize}) : uploaded file size({_blobSize})"
            )
            return _fileSize == _blobSize

        # 원본 파일의 row count와 GCS 파일의 row count 비교
        def fn_check_file_row_count(_fileName):
            fileRowCount = 0
            blobRowCount = 0

            # Count rows
            blobRowCount = len(blob.download_as_string().splitlines())

            # file의 row count 측정
            def _fnmakegen(reader):
                while True:
                    b = reader(2 ** 16)
                    if not b:
                        break
                    yield b

            with open(_fileName, "rb") as f:
                fileRowCount = sum(buf.count(b"\n")
                                   for buf in _fnmakegen(f.raw.read))

            logger.info(
                f"original file row count ({fileRowCount}) : uploaded file row count({blobRowCount})"
            )
            return fileRowCount == blobRowCount

        if (type == "both"):
            if (fn_check_file_size(fileName) & fn_check_file_row_count(fileName)):
                rtn = True

        else:
            if (type == "rowcnt"):
                if (fn_check_file_row_count(fileName)):
                    rtn = True

            if (type == "bytesize"):
                if (fn_check_file_size(fileName)):
                    rtn = True

        # 업로드 정합성 이상이 없을 시 에러 파일 리스트에서 제거
        if rtn == False:
            errLogger.error(f"{fileName} is not uploaded to GCS")
        else:
            self.errFiles.remove(fileName)

        # 업로드 정합성 확인 결과 리턴
        return rtn

    # GCS에 파일 업로드하는 함수
    def fn_upload_blob_to_gcs(self, bucket, sourceFileName, destinationBlobName):
        logger.info("fnUploadBlobToGcs")
        blob = bucket.blob(destinationBlobName, chunk_size=self.chunkSize)

        with open(sourceFileName, "rb") as inFile:
            totalSize = os.fstat(inFile.fileno()).st_size
            with tqdm.wrapattr(inFile, "read", total=totalSize, miniters=1, desc=f"File {sourceFileName} uploaded to {destinationBlobName}.") as fileObj:
                fileObj.seek(0)
                blob.upload_from_file(
                    fileObj,
                    size=totalSize,
                )
        if (self.fn_check_file_in_gcs(bucket, sourceFileName,
                                  destinationBlobName, type="both")):
            logger.info(
                f"File {sourceFileName} uploaded to {destinationBlobName}."
            )


    

    ### GCS String 업로드 부분 ####
    def fn_string_upload(self ,jsonlines_data , destinationName , typeLogger , status):
        self.upload_cnt += 1
        try : 
            # gcs_project_id = ""
            # gcs_bucket_name = ""
            # gcs_key_file_name = None
        
            if status == "test" : 
                gcs_project_id = GCS_PROJECT_ID
                gcs_bucket_name = GCS_BUCKET_NAME
                gcs_key_file_name = GCS_KEY_FILE

            print(status)
            
            storageClient = storage.Client.from_service_account_json(
                    gcs_key_file_name, project=gcs_project_id)
            
            print(storageClient, " storageclient")
            
            bucket = storageClient.bucket(gcs_bucket_name)
           
            # GCS 에 파일 업로드 하기

            blob = bucket.blob(destinationName)

            blob.upload_from_string(jsonlines_data)
        
            # def fnCheckContentsInGcs(self, bucket, org_Contents_Rows, blobName , typeLogger):
            typeLogger.info("complete upload_from_string")
            
            cntBlob = bucket.get_blob(destinationName)
            blobRowCount = len(cntBlob.download_as_string().splitlines())
            typeLogger.info("org_data_count : " + str(jsonlines_data.count('\n')) + " upload_data_count : " + str(blobRowCount) )

            
            if(jsonlines_data.count('\n') != blobRowCount) :
                typeLogger.info("not Equals --- org_data_count : " + str(jsonlines_data.count('\n')) + " upload_data_count : " + str(blobRowCount) )
                if(self.upload_cnt < 5) : 
                    if(self.upload_cnt > 1) :
                        sleep(600)
                    self.fn_string_upload(jsonlines_data , destinationName , typeLogger)
            else :
                return True
        except Exception as e:
            typeLogger.info(f"fnStringUpload Error Raise : {e}")
            if(self.upload_cnt < 5) : 
                sleep(600)
                self.fn_string_upload(jsonlines_data , destinationName , typeLogger , status)
            return False
    
