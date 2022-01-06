from datetime import datetime

class Logger():
    '''
        Used to save logs into a file
    '''
    def __init__(self,file='logfile.log'):
        self.f_name=file
    def log_operation(self,log_type,log_message):
        '''
        :param log_type:
        :param log_message:
        :return: None
        '''
        now = datetime.now()
        current_time = now.strftime("%d-%m-%Y %H:%M:%S")
        f = open(self.f_name, "a")
        f.write(current_time + "," + log_type + "," + log_message + "\n")
        f.close()
