from apscheduler.schedulers.blocking import BlockingScheduler
import sys
from svc.common import errlogger
from svc.common import utils
from svc.common import env
from svc.common import upload
from svc.model import aws
from svc.model import bigquery
from svc.model import initial_data
import json
import traceback


## ======================| [end] Network Test |======================

ENV_PATH = ''


errlogger.Logger.fnInitLogger()
logger = errlogger.Logger("info").logger
errLogger = errlogger.Logger("error").logger
initial_data_Logger = errlogger.Logger("initial_data").logger
awsLogger = errlogger.Logger('aws').logger

INNER_SAVED_DIRECTORY = ""
if(utils.get_os_name() == 'Linux'):
    INNER_SAVED_DIRECTORY = "/Linux/Share/"
elif(utils.get_os_name() == 'Windows'):
    INNER_SAVED_DIRECTORY = "datas/mail/"


UP_LIST = ['test']


try : 
    if __name__ == '__main__':
        arguments = sys.argv
        
        if len(arguments) == 1:
            # TODO .env file load
            ENV_PATH = '.env' 
            print(">>>>>>>>>>>>>test 모드>>>>>>>>>>>>>")
        elif arguments[1].lower() == "-d" or arguments[1].lower() == "-dev":
            ENV_PATH = '.env'
            print(">>>>>>>>>>>>>개발 모드>>>>>>>>>>>>>")
            
        elif arguments[1].lower() == "initial_data" :
            ENV_PATH = '.env' 
            print(">>>>>>>>>>>>> initial_data >>>>>>>>>>>>>")
            for status in UP_LIST :
                initial_data.Initial_Data(arguments[1].lower() , status)
                
        elif arguments[1].lower() == "aws" :
            ENV_PATH = '.env' 
            for status in UP_LIST :
                print(">>>>>>>>>>>>> aws >>>>>>>>>>>>>")
                aws.AWS_DATA(arguments[1].lower() , status)
        elif arguments[1].lower() == "bigquery" :
            ENV_PATH = '.env'
            print(">>>>>>>>>>>>> bigquery >>>>>>>>>>>>>")
            bg_client = bigquery.Bg_Client()
            print(bg_client.query_select())
            
        elif arguments[1].lower() == "up_table_check" :
            ENV_PATH = '.env'  
            print(">>>>>>>>>>>>> up_table_check >>>>>>>>>>>>>")
            gcsUpload = upload.Upload([])
            gcsUpload.table_row_cnt_checker("DEV")
            
        else:
            ENV_PATH = '.env'  
            print(">>>>>>>>>>>>>운영 모드>>>>>>>>>>>>>")
            
except Exception as e :
    print(e)
    traceback.print_exc()