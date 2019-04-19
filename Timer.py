

from time import time
class Timer:
    """
    a simple class for printing time (s) since init
    """
    def __init__(self):
        self.t0=time()
    
    def start(self,name="Unknow"):
        self.name=name
        print("TT Start> "+name+"...")
        self.t0=time()
        
    def stop(self):
        t1=time()
        dt=t1-self.t0
        print("done in {:2.1f}s\n".format(dt))
        return dt
