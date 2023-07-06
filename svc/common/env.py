from os import environ
from dotenv import load_dotenv

import os
# TODO - Load .env file
#ENV_PATH = '.env'
ENV_PATH = '.env.dev'

load_dotenv()

GCS_PROJECT_ID = str(os.environ.get("GCS_PROJECT_ID"))
GCS_BUCKET_NAME = str(os.environ.get("GCS_BUCKET_NAME"))
GCS_KEY_FILE = str(os.environ.get("GCS_KEY_FILE"))


