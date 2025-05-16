import pandas as pd

from ETL.Interfaces.ExtractorInterface import Extractor


class AWSSingleFileExtractor(Extractor):

    def __init__(self, filepath:str, compression:str='infer', creds=None):
        self.filepath = filepath
        self.creds = creds
        self.compression = compression

    def read_data(self):
        df = pd.read_csv(filepath_or_buffer=self.filepath, compression=self.compression)
        return df