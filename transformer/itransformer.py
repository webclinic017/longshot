from abc import ABCMeta, abstractmethod

class ITransformer(metaclass=ABCMeta):

    @classmethod
    @abstractmethod
    def __subclasshook__(cls,subclass):
        return (hasattr(subclass,"extract") and callable(subclass.extract))

    @abstractmethod
    def transform(self,data):
        raise NotImplementedError("must define transform to use this interface")