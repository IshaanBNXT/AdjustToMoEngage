import abc

class Transformer(abc.ABC):
    
    @abc.abstractmethod
    def transform_data(self, data):
        pass