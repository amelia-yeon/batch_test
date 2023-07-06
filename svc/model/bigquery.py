import glob
import os 
from google.cloud import bigquery
from google.oauth2 import service_account
from svc.common import env
import logging

# TODO - Load .env file
#ENV_PATH = '.env'
logger = logging.getLogger("info")
errLogger = logging.getLogger("error")
GCS_KEY_FILE = str(os.environ.get("GCS_PROJECT_ID"))


class Singleton_Meta(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton_Meta, cls).__call__(*args, **kwargs)
        return cls._instances[cls]
    
    
class Bg_Client(metaclass = Singleton_Meta):
    
    client = None
    
    # Google Service Key 얻어옴
    key_path = GCS_KEY_FILE

    def __init__(self ):
        # Credentials 객체 생성
        credentials = service_account.Credentials.from_service_account_file(self.key_path)        
            
        # GCP 클라이언트 객체 생성
        client = bigquery.Client(credentials = credentials, project = credentials.project_id)
        self.client = client

    def query_select(self) :
        print("bigQuery Select")
        
        # 데이터 조회 쿼리
        sql = f"""
        SELECT * FROM `mpp-biz-dev.plcc_test.test1` LIMIT 10
        """

        # 데이터 조회 쿼리 실행 결과
        query_data = self.client.query(sql).to_dataframe()
        data = query_data.to_dict('records')
        print(query_data)
        print(type(query_data), "query_data_type>>>>>>>>>>>>>")
        
        return data

   
