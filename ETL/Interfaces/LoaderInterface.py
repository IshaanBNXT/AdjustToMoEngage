import abc

class Loader(abc.ABC):
    
    @abc.abstractmethod
    def upload_data(self, data, destination):
        pass