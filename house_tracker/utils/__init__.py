
def singleton(cls):
    def inner_func(*args, **kwargs):
        if inner_func.instance is None:
            inner_func.instance = cls(*args, **kwargs)
        return inner_func.instance
        
    inner_func.instance = None
    return inner_func
    
    
class SingletonMeta(type):
    def __call__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = type.__call__(cls, *args, **kwargs)
        return cls.instance
