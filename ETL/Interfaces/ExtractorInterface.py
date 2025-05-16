import abc

class Extractor(abc.ABC):
    
    @abc.abstractmethod
    def read_data(self):
        pass