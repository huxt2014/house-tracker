
class singleton():
    """
    Implement the singleton pattern
    """
    
    def __init__(self, aClass):
        self.aClass = aClass
        self.instance  = None
    
    def __call__(self, *args, **kwargs):
        if self.instance == None:
            self.instance = self.aClass(*args, **kwargs)
        return self.instance
    