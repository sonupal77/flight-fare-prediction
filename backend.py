from Database import Connector
from Logger import Logger
from App import predict
import threading
logging = Logger('logFile.log')
class ThreadWithResult(threading.Thread):
    def __init__(self, group=None, target=None, name=None, args=(), kwargs={}, *, daemon=None):
        def function():
            self.result = target(*args, **kwargs)
        super().__init__(group=group, target=function, name=name, daemon=daemon)

def Backend(result):
    """
    :DESC: This function send data into database. If table is not present it creates one and adds data.
    :param result: provided by Thread creater
    :return: None
    """

    logging.log_operation('INFO', 'Data for Database: {}'.format(result))
    result['Journey_month'] = int(result['Journey_month'])
    result['Journey_day'] = int(result['Journey_day'])
    result['Total_Duration'] = int(result['Total_Duration'])

    load_data = Connector()
    try:
        load_data.master()
        load_data.addData(result)
    except:
        load_data.addData(result)
    finally:
        logging.log_operation('INFO', 'Data retrieved')

def getResult(result):
    logging.log_operation('INFO', 'Threading Called !')
    in1 = result
    in2 = result
    thread1 = ThreadWithResult(target=predict, args=(in1,))
    thread2 = ThreadWithResult(target=Backend, args=(in2,))
    logging.log_operation('INFO', 'Threading Created !')
    thread1.start()
    thread2.start()
    logging.log_operation('INFO', 'Threading Started !')
    thread1.join()
    thread2.join()
    logging.log_operation('INFO', 'Threading join !')
    return result