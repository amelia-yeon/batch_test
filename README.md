# venv 실행 명령어
cd .. & python -m venv [folder_name] & cd [folder_name] & .\Scripts\activate & pip install -r requirements.txt

# venv deactivate 명령어
deactivate

# 설치 명령어
pip install -r requirements.txt


# 실행 함수
## 개발
- python main.py -d
- python main.py -dev
- python main.py -D
- python main.py -Dev

## 운영
- python main.py

## 초기 실행시
## init_data 소스 실행시 : python main.py init_data (스페이스바 구분으로 main.py 뒤에 word 가 argunment )