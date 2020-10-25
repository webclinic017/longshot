from abc import ABCMeta, abstractmethod

## sources https://realpython.com/python-interface/
## https://docs.python.org/3/library/abc.html
class IExtractor(metaclass=ABCMeta):
    @classmethod
    def __subclasshook__(cls,subclass):
        return (hasattr(subclass,"extract") and callable(subclass.extract))

    @abstractmethod
    def extract(self,api,params):
        raise NotImplementedError("must define extract to use this interface")