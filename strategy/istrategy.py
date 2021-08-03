from abc import ABCMeta, abstractmethod

class IStrategy(metaclass=ABCMeta):
    @classmethod
    def __subclasshook__(cls,subclass):
        return (hasattr(subclass,"transform") and callable(subclass.transform)
        and hasattr(subclass,"create_sim") and callable(subclass.create_sim)
        and hasattr(subclass,"backtest") and callable(subclass.backtest)
        and hasattr(subclass,"create_rec") and callable(subclass.create_rec)
        and hasattr(subclass,"store_models") and callable(subclass.store_models)
        and hasattr(subclass,"reset_db") and callable(subclass.reset_db))
    
    @abstractmethod
    def transform(self):
        raise NotImplementedError("must define transform to use this interface")
    
    @abstractmethod
    def create_sim(self):
        raise NotImplementedError("must define create_sim to use this interface")
    
    @abstractmethod
    def backtest(self):
        raise NotImplementedError("must define backtest to use this interface")
    
    @abstractmethod
    def create_rec(self):
        raise NotImplementedError("must define create_rec to use this interface")
    
    @abstractmethod
    def store_models(self):
        raise NotImplementedError("must define store_models to use this interface")
    
    @abstractmethod
    def reset_db(self):
        raise NotImplementedError("must define reset_db to use this interface")