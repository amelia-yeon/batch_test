from datetime import datetime
import sys
import logging
from . import utils



class Logger():
    def __init__(self, type=None):
        self.type = type
        self.logger = self.fnSetupLogger()

    # logger 초기 셋팅
    def fnInitLogger():
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

    # logger 셋팅 함수
    # self.type 에 따라 파일 명이 변경되어 log 쌓임
    def fnSetupLogger(self):
        if self.type == None:
            return
        
        loggerFilePath = "log"
        loggerFileName = "sy-project"
        filePrefix = datetime.now().strftime("%Y_%m_%d")+"_"
        loggerLevel = logging.INFO

        
        if (self.type == "error"):
            loggerFilePath += "/error"
            loggerFileName += "_err"
            loggerLevel = logging.WARNING
        else :
            loggerFileName += "_" + self.type

        loggerFileName += ".log"
        # 로깅 저장 파일 디렉토리 생성
        utils.fnmkdir(loggerFilePath,
                      f"Error: Failed to create the log({self.type}) directory.")

        logger = logging.getLogger(self.type)
        logger.setLevel(loggerLevel)
        formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s : %(message)s',
                                      '%m-%d-%Y %H:%M:%S')

        stdoutHandler = logging.StreamHandler(sys.stdout)
        stdoutHandler.setLevel(loggerLevel)
        stdoutHandler.setFormatter(formatter)

        utils.fnmkdir(loggerFilePath,
                      "Error: Failed to create the log directory.")

        fileHandler = logging.FileHandler(
            filename=loggerFilePath+"/"+filePrefix+loggerFileName, encoding="utf-8", mode="a+")
        fileHandler.setLevel(loggerLevel)
        fileHandler.setFormatter(formatter)

        logger.addHandler(fileHandler)
        logger.addHandler(stdoutHandler)
    
        return logger
