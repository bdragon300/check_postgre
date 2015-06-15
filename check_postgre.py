##
# Name:     check_postgre.py
# Created:  2015-06-05
# Author:   Igor Derkach
# Email:    id@miran.ru
# Summary:  Nagios NRPE plugin to check PostgreSQL server health and to return its performance
##
__author__      = 'Igor Derkach'

#
# Begin code

import sys, os
import ConfigParser, tempfile
from optparse import OptionParser

def err(msg, status=1):
    print 'ERROR: ' + msg
    sys.exit(status)

try:
    import psycopg2
except:
    err('No module python-psycopg2 found')


class DBConnection:
    '''
    Simple class to handle database connections and to process queries
    '''

    # Connection object
    _conn = False

    # Parameters what uses to connect to the database
    _connParams = {}


    def queryAll(self, q):
        """
        Fetches all data in one shot and returns it
        :param q: query
        :return: list of tuples with query result rows
        """
        cur = self._conn.cursor()
        cur.execute(q)
        if cur.rowcount:
            res = cur.fetchall()
        else:
            res = ()

        cur.close()
        return res

    def queryOne(self, q):
        """
        Fetches only first value from the first row
        :param q: query
        :return: value
        """
        cur = self._conn.cursor()
        cur.execute(q)
        if cur.rowcount:
            res = cur.fetchone()[0]
        else:
            res = None

        cur.close()
        return res

    def getConnectionParams(self):
        return self._connParams


    def __init__(self, dbname, host, port, user, password):
        self._conn = psycopg2.connect(database=dbname, host=host, port=port, user=user, password=password)
        self._connParams = {'dbname': dbname, 'host': host, 'port': port, 'user': user, 'password' : password}

    def __del__(self):
        if self._conn != False:
            self._conn.close()


class TempConfig:
    """
    Stores and loads results from previous plugins check file
    """

    _f = None # File object
    _cfg = None # ConfigParser object
    _section = 'Counters' # Section in last check file
    _data = []

    def __init__(self, dirname, filename):
        """
        Creates temp file
        :return:
        """
        if dirname != None:
            tdir = os.path.join(tempfile.gettempdir(), dirname)
        else:
            tdir = tempfile.gettempdir()
        if not os.path.isdir(tdir):
            os.mkdir(tdir)
        tfile = os.path.join(tdir, filename)
        if not os.path.isfile(tfile):
            self._f = open(tfile, 'w')
            self._f.close()
        self._f = open(tfile, 'rw')

        self._cfg = ConfigParser.ConfigParser()

    def __del__(self):
        if self._f != None:
            self._f.close()

    def __iter__(self):
        yield self._data

    def __getitem__(self, item):
        try:
            return self._data[item]
        except:
            return []

    def load(self, data):
        self._cfg.readfp(self._f)
        for i in self._cfg.sections():
            self._data[i] = self._cfg.items(i,vars=True)
        self._f.close()

    def flush(self):
        """
        Writes data to temp file
        """
        cfg = ConfigParser.ConfigParser()
        for i in self._data:
            if not isinstance(self._data[i], dict):
                raise TypeError('data[] item must has dict type')
            for j in self._data[i]:
                cfg.set(i, j, self._data[i][j])

        cfg.write(self._f)


class Pg:
    """
    Base class for Pg* classes that retrieve info from postgres
    """

    _lastCheckFile = None
    
    # DBConnection object
    _connection = None

    def __init__(self, connection, lastCheckFile):
        """
        :type connection: DBConnection
        :type lastCheckFile: TempConfig
        """
        self._connection = connection
        self._lastCheckFile = lastCheckFile

    def diffWithLastCheck(self, section, name, value):
        """
        Returns difference between current value and last check value
        If no value was saved then simply return the current one
        :param section:
        :param name:
        :param value: numeric
        :return:
        """
        try:
            return value - self._lastCheckFile[section][name]
        except:
            return value

    def getDatabaseName(self):
        return self._connection.getConnectionParams()['dbname']


class PgPostmaster(Pg):
    '''
    Retirieves information about PostgreSQL instance
    '''

    def getUptime(self):
        """
        Returns uptime string like '060d 14:16:54'
        :return: uptime string
        """
        q = 'SELECT to_char(now() - pg_postmaster_start_time(), \'DDD"d" HH24":"MI":"SS\') uptime;'
        res = self._connection.queryAll(q)
        try:
            return res[0][0]
        except:
            raise Exception('Unable to get server uptime')

    def getConnSummary(self):
        """
        Returns connections count, client summary by IP and username
        :return: summary dict {'count', 'users', 'addrs'}
        """
        # Connections count
        res = {}

        q = "SELECT COUNT(*) FROM pg_stat_activity"
        res['count'] = self._connection.queryOne(q)

        # Users count
        q = "SELECT usename, COUNT(*) as cnt FROM pg_stat_activity GROUP BY usename ORDER BY cnt DESC"
        res['users'] = self._connection.queryAll(q)

        # IP count
        q = "SELECT client_addr, COUNT(*) as cnt FROM pg_stat_activity GROUP BY client_addr ORDER BY cnt DESC"
        res['addrs'] = self._connection.queryAll(q)

        return res

    # def getDiskCacheInfo(self):
    #     """
    #     Returns disk cache efficiency
    #     :return:
    #     """
    #     q = "select *," \
    #         "(cast((blks_hit) as float)/(blks_hit+blks_read))as eff " \
    #         "from pg_stat_database " \
    #         "order by eff asc"
    #     res = self._connection.queryAll(q)
    #
    #     return res

class PgDatabase(Pg):
    '''
    Retrieves information about one database
    '''

    def getDiskCacheInfo(self):
        """
        Returns disk read blocks and cache hits and efficiency of cache (for tables and indexes)
        :return: info list of tuples: all pg_statio_user_tables columns + eff
        """
        q = "select datname, " \
            "blks_read, " \
            "blks_hit," \
            "(cast((blks_hit) as float)/(blks_hit+blks_read))as eff " \
            "from pg_stat_database " \
            "where datname = '%s'" % (self.getDatabaseName())
        # q = "select *," \
        #     "(cast((idx_blks_hit+heap_blks_hit) as float)/(idx_blks_hit+heap_blks_hit+idx_blks_read+heap_blks_read))as eff  " \
        #     "from pg_statio_user_tables " \
        #     "WHERE idx_blks_hit+idx_blks_read+heap_blks_read+heap_blks_hit>0 " \
        #     "ORDER BY eff ASC;"
        res = self._connection.queryAll(q)
        return [res]

    def getTableIndexEfficiencyTop(self):
        """
        Returns list of indexes sorted by ratio of tables' seq_scan and indexes' idx_scan
        :return: list of tuples
        """
        q = "select relname, " \
            "seq_scan, " \
            "idx_scan, " \
            "(cast(idx_scan as float)/nullif((seq_scan+idx_scan), 0)) as eff " \
            "from pg_stat_user_tables " \
            "ORDER BY eff ASC"
        res = self._connection.queryAll(q)
        return res

    def getTupleLoadTop(self):
        """
        Returns list of tables sorted by tuples read/modified
        :return: list of tuples
        """
        q = "select datname, " \
            "tup_fetched," \
            "(tup_inserted+tup_updated+tup_deleted) as tup_modified " \
            "from pg_stat_database " \
            "where datname = '%s' " \
            "order by tup_fetched desc, tup_modified desc" % (self.getDatabaseName())
        res = self._connection.queryAll(q)
        return res


# Command line options
usage = 'Usage: %prog [options]'
cliparse = OptionParser(usage=usage, add_help_option=False)
cliparse.add_option("-d", "--dbname", dest="dbname", action="append", help="databases to monitor to. Can be specified multiple times")
cliparse.add_option("-h", "--host", dest="host", default="127.0.0.1", help="database server host (default: 127.0.0.1)")
cliparse.add_option("-U", "--username", dest="username", default="postgres", help="database user name (default: postgres)")
cliparse.add_option("-p", "--port", dest="port", default="5432", help="database server port (default: 5432)")
cliparse.add_option("-W", "--password", dest="password", help="password to connect to")
cliparse.add_option("--diskstat", dest="diskstat", default="postgres", action="store_true", help="display disk cache usage in Performance")
cliparse.add_option("--tupstat", dest="tupstat", default="postgres", action="store_true", help="display tuples r/w in Performance")
cliparse.add_option("--indstat", dest="indstat", default="postgres", action="store_true", help="display index efficiency in Performance")
cliparse.add_option("--help", action="help")
(cliopt, cliargs) = cliparse.parse_args()

if (cliopt.dbname == None):
    err("No databases to monitor. Specify at least one by -d option")

lastCheck = TempConfig('check_postgre', 'last_check')
conns = {}
dbs = {}
for i in cliopt.dbname:
    conns[i] = DBConnection(i, cliopt.host, cliopt.port, cliopt.username, cliopt.password)
    dbs[i] = PgDatabase(conns[i], lastCheck)

if len(conns) == 0:
    raise Exception('No connections found')

pm = PgPostmaster(conns.values()[0], lastCheck)

message = 'Uptime:' + pm.getUptime() + ' / '


res = pm.getConnSummary()

# [(a,b)(...)] -> "a=b,..."
ipsstr = ','.join(map(lambda x: "%s=%s" % (x[0], x[1]), res['addrs'][0:2]))
usersstr = ','.join(map(lambda x: "%s=%s" % (x[0], x[1]), res['users'][0:2]))
message += "instances:%s / topIPs:%s / topusers:%s" % (res['count'], ipsstr, usersstr)

performance = 'DB:'
pieces = []
for i in dbs:
    s = []
    if cliopt.diskstat:
        res = dbs[i].getDiskCacheInfo()[0]
        s.append("diskread:%s,cachehit:%s(%s%%)" % (res[0][1], res[0][2], int(float(res[0][3]) * 100)))
    if cliopt.tupstat:
        res = dbs[i].getTupleLoadTop()
        s.append("tupfetch:%s,tupmod:%s" % (res[0][1], res[0][2]))
    if cliopt.indstat:
        res = dbs[i].getTableIndexEfficiencyTop()
        top3 = ','.join(map(lambda x: "%s=seqscan:%s,indscan:%s(%s%%)" % (x[0], x[1], x[2], x[3]), res[0:2]))
        s.append("TABLES:{" + top3 + "}")
    pieces.append("[%s=" % dbs[i].getDatabaseName() + ','.join(s) + '] ')
performance += ''.join(pieces)

print message + ' | ' + performance
sys.exit(0) #TODO: return other statuses

#TODO: queries per second
#TODO: check whether postgres statistics enabled
#TODO: check permission to read statistics