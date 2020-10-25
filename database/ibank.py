from abc import ABCMeta, abstractmethod

## sources https://realpython.com/python-interface/
## https://docs.python.org/3/library/abc.html
class IBank(metaclass=ABCMeta):
    @classmethod
    def __subclasshook__(cls,subclass):
        return (hasattr(subclass,"connect") and callable(subclass.connect)
        and hasattr(subclass,"close") and callable(subclass.close)
        and hasattr(subclass,"store_data") and callable(subclass.store_data)
        and hasattr(subclass,"retrieve_data") and callable(subclass.retrieve_data))
    
    @abstractmethod
    def connect(self):
        raise NotImplementedError("must define connect to use this interface")
    
    @abstractmethod
    def close(self):
        raise NotImplementedError("must define close to use this interface")
    
    @abstractmethod
    def store_data(self):
        raise NotImplementedError("must define store_data to use this interface")
    
    @abstractmethod
    def retrieve_data(self):
        raise NotImplementedError("must define retrieve_data to use this interface")