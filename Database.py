from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from Logger import Logger
logging = Logger('logFile.log')
class Connector:
    def __init__(self):
        """
        :DESC: Creates connection with Database when backend thread runs.
        """
        logging.log_operation('INFO', 'Obj created')
        self.Client_id = 'cFDkqvpEASdTTSjGlxcahZQu'
        self.Client_secret = 'pTjo0DOn2y0ZPvaF7wk1BzxX0lcF0.yWIAtQ8Tq-7j9xRvLE0z11ZntZItWU-wHldDcUeEJHkdiv6,mPZLnBb5,zo0QJ8Uq6bZieU2DbqkjpPq7XsQkOOhp0w,qit58b'
        cloud_config = {'secure_connect_bundle': 'F:\\flight\\secure-connect-flightfare.zip'}
        auth_provider = PlainTextAuthProvider(self.Client_id, self.Client_secret)
        cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        self.session = cluster.connect()

    def master(self):
        """
        :DESC: Creates table if not existed into database
        :return:
        """
        self.session.execute("use flight")
        self.session.execute("select release_version from system.local")
        self.session.execute("CREATE TABLE flightfare18(id uuid PRIMARY KEY,Airline text,Source text,Destination text,Total_Stops int,Departure date,Output int);")

    def addData(self, result):
        """
        :param result: Gets data from user and puts it into database
        :return:
        """
        logging.log_operation('INFO', "Inside addData")
        logging.log_operation('INFO', "Inside addData")

        column = "id, Airline, Source,Destination, Total_Stops, Total_Duration, Journey_month, Journey_day"
        value = "{0},'{1}','{2}','{3}',{4},{5},{6}".format('uuid()', result['airline'], result['Source'],
                                                                 result['Source2'], result['output'],
                                                                 result['Total_stops'], result['date_dep'],
                                                                )
        logging.log_operation('INFO', "String created")
        custom = "INSERT INTO flightfare18({}) VALUES({});".format(column, value)

        logging.log_operation('INFO', "Key created")
        self.session.execute("USE flight")

        output = self.session.execute(custom)

        logging.log_operation('INFO', "Column inserted {}".format(output))


    def getData(self):
        """
        :DESC: Retrieves Data from Database
        :return:
        """
        self.session.execute("use flight")
        row = self.session.execute("SELECT * FROM flightfare18;")
        collection = []
        for i in row:
            collection.append(tuple(i))
        logging.log_operation('INFO', "Retrieved Data from Database : {}".format(i))
        return tuple(collection)