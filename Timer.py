

from time import time
class Timer:
    """
    a simple class for printing time (s) since init
    """
    def __init__(self):
        self.t0=time()
    
    def start(self,name="Unknow"):
        self.name=name
        print("Starting: "+name)
        self.t0=time()
        
    def stop(self):
        t1=time()
        dt=t1-self.t0
        print(self.name+" done in {:2.1f}s".format(dt))
        return dt
