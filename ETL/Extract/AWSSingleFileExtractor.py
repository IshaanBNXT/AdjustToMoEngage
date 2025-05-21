import pandas as pd
import os
import datetime
from dotenv import load_dotenv
load_dotenv()

from ETL.Interfaces.ExtractorInterface import Extractor


class AWSSingleFileExtractor(Extractor):

    def __init__(self, file_date:datetime.date=datetime.date.today(), compression:str='infer', creds=None):
        self.file_date = file_date
        self.creds = creds
        self.compression = compression
        self.filepath = None

    def read_data(self):
        self.set_filepath()
        df = pd.read_csv(filepath_or_buffer=self.filepath, compression=self.compression)
        return df
    
    def set_filepath(self):
        if self.filepath is None:
            folder = os.environ.get("AWS_S3_BUCKET_PATH")
            file_prefix = os.environ.get("ADJUST_DAILY_FILE_PREFIX")
            file_ext = os.environ.get("ADJUST_DAILY_FILE_EXTENSION")
            file_date_str = self.file_date.strftime("%d-%m-%Y")
            filepath = folder + file_prefix + file_date_str + file_ext
            self.filepath = filepath