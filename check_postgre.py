##
# Name:     check_postgre.py
# Created:  2015-06-05
# Author:   Igor Derkach
# Email:    id@miran.ru
# Summary:  Nagios NRPE plugin to check PostgreSQL server health and to return its performance
##
__author__ = 'Igor Derkach'

#
# Begin code

import sys
import os
import ConfigParser
import tempfile
from optparse import OptionParser


class Output:
    """
    The class must be singleton
    Collects various types of script messages and eventually prints them and exists with correct exit code
    """
    _msgs = {'warn': [], 'crit': [], 'unknown': []}
    _output = {'message': [], 'performance': []}

    # Appropriate types of messages
    # At the end we call finish() to print message(s) and exit with correct exit code
    def msg(self, msg):
        self._output['message'].append(str(msg).replace("\n", ""))

    def perf(self, msg):
        self._output['performance'].append(str(msg).replace("\n", ""))

    def warn(self, msg):
        self._msgs['warn'].append(str(msg).replace("\n", ""))

    def crit(self, msg):
        self._msgs['crit'].append(str(msg).replace("\n", ""))

    def unknown(self, msg):
        self._msgs['unknown'].append(str(msg).replace("\n", ""))

    def finish(self):
        """
        Prints message to stdout. If errors, warnings, unknown messages has occured then
         exists with those messages and with appropriate exit codes
        :return:
        """
        if len(self._msgs['crit']):
            print 'CRITICAL: ' + ';'.join(self._msgs['crit'])
            sys.exit(2)
        elif len(self._msgs['warn']):
            print 'WARNING: ' + ';'.join(self._msgs['warn'])
            sys.exit(1)
        elif len(self._msgs['unknown']):
            print 'UNKNOWN: ' + ';'.join(self._msgs['unknown'])
            sys.exit(3)
        else:
            sys.stdout.write('OK: ')
            if len(self._output['message']):
                sys.stdout.write(';'.join(self._output['message']))
            if len(self._output['performance']):
                sys.stdout.write(' | ' + ';'.join(self._output['performance']))
            if len(self._output['message']) == 0:
                sys.stdout.write('All OK')

            sys.stdout.write("\n")
            sys.exit(0)


output = Output()

try:
    import psycopg2
except:
    output.unknown('No module psycopg2 found')
    output.finish()


class DBConnection:
    """
    Simple class to handle database connections and to process queries
    """

    # Connection object
    _conn = None

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

    def getDatabaseName(self):
        return self._connParams['dbname']

    def __init__(self, dbname, host, port, user, password):
        self._conn = psycopg2.connect(database=dbname, host=host, port=port, user=user, password=password)
        self._connParams = {'dbname': dbname, 'host': host, 'port': port, 'user': user, 'password': password}

    def __del__(self):
        if self._conn is not None:
            self._conn.close()


class TempConfig:
    """
    Stores and loads results from previous plugins check file
    """

    _filename = None
    _data = {}

    def __init__(self, filename, dirname=None):
        """
        Creates temp file
        :return:
        """
        f = None
        if dirname is not None:
            tdir = os.path.join(tempfile.gettempdir(), dirname)
        else:
            tdir = tempfile.gettempdir()
        if not os.path.isdir(tdir):
            os.mkdir(tdir)
        tfile = os.path.join(tdir, filename)
        self._filename = tfile
        if not os.path.isfile(tfile):
            f = open(tfile, 'w')
            f.close()

    def __iter__(self):
        return iter(self._data)

    def __getitem__(self, item):
        try:
            return self._data[item]
        except KeyError:
            return []

    def __setitem__(self, key, value):
        self._data[key] = value

    def load(self):
        f = open(self._filename, 'r')
        cfg = ConfigParser.ConfigParser()
        cfg.readfp(f)
        for i in cfg.sections():
            self._data[i] = dict(cfg.items(i))
        f.close()

    def flush(self):
        """
        Writes data to temp file
        """
        cfg = ConfigParser.ConfigParser()
        f = open(self._filename, 'w')
        for i in self._data:
            if not isinstance(self._data[i], dict):
                raise TypeError('data[] item must has dict type')
            for j in self._data[i]:
                if not cfg.has_section(i):
                    cfg.add_section(i)
                cfg.set(i, j, self._data[i][j])

        cfg.write(f)
        f.flush()
        f.close()


class ConfigMemento:

    _tempConfig = None

    _members = []
    _state = {}

    def __init__(self, filename, dirname):
        self._tempConfig = TempConfig(filename, dirname)
        self._tempConfig.load()
        for i in self._tempConfig:
            self._state[i] = self._tempConfig[i]

    def subscribe(self, obj):
        """
        :type obj: Pg*
        """
        if obj not in self._members:
            self._members.append(obj)

    def unsubscribe(self, obj):
        for i in self._members:
            if i == obj:
                self._members.remove(i)

    def saveToFile(self):
        for i in self._members:
            self._state[i._stateIndex] = i._state
            self._tempConfig[i._stateIndex] = i._state

        self._tempConfig.flush()

    def loadState(self):
        for i in self._members:
            self.loadConfigTo(i)

    def loadConfigTo(self, obj):
        if obj._stateIndex in self._state:
            obj._state = self._state[obj._stateIndex]
        else:
            obj._state = {}


class Pg:
    """
    Base class for Pg* classes that retrieve info from postgres
    """
    
    # DBConnection object
    _connection = None

    # Internal state of object to store to the temp file
    # Key-value pairs
    _state = {}

    # Section name to write to temp file. In general - database name
    # For postmaster instance is "POSTMASTER"
    _stateIndex = ""

    def __init__(self, connection, memento):
        """
        :type connection: DBConnection
        :type memento: ConfigMemento
        """
        self._connection = connection
        self._stateIndex = connection.getDatabaseName()
        memento.subscribe(self)
        memento.loadConfigTo(self)

    def diffWithLastCheck(self, name, value):
        """
        Returns difference between current value and last check value
        If no value was saved then simply return the current one
        :param name:
        :param value: numeric
        :return:
        """
        try:
            return value - int(self._state[name])
        except:
            return value

    def getDatabaseName(self):
        return self._connection.getConnectionParams()['dbname']

    def _storeLastCheckValue(self, name, value):
        self._state[name] = value

    @staticmethod
    def _truncateNumber(number):
        """
        Truncate big number and adds prefix: K for thousands, M for millions
        :param number:
        :return: string
        """
        p = ['', 'K', 'M', 'MM']

        while number > 1000 and len(p) > 0:
            number /= 1000
            p.pop(0)

        return str(int(number)) + p[0]


class PgPostmaster(Pg):
    """
    Retirieves information about PostgreSQL instance
    """

    _stateIndex = 'POSTMASTER'

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

    def getQps(self):
        """
        Returns queries per second
        This value calculates from previous check value (difference)
        :return: [raw_value, 'raw value for output', 'difference for output']
        """
        q = "select extract(epoch from now())::int as epoch, sum(xact_commit+xact_rollback) as sum " \
            "from pg_stat_database"
        res = self._connection.queryAll(q)[0]

        try:
            r = float(self.diffWithLastCheck('qps', res[1])) / self.diffWithLastCheck('qpstime', res[0])
        except ZeroDivisionError:
            r = self.diffWithLastCheck('qps', res[1])

        self._storeLastCheckValue('qps', res[1])
        self._storeLastCheckValue('qpstime', res[0])
        return [res, self._truncateNumber(res[1]), self._truncateNumber(r)]

    def isStatEnabled(self):
        """
        Checks whether statistics enabled in postgresql config
        Only track_counts checks
        :return: boolean
        """
        q = "select setting from pg_settings where name='track_counts'"
        res = self._connection.queryOne(q)
        return res == 'on'

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
    """
    Retrieves information about one database
    """

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


# Command line options parse
usage = 'Usage: %prog [options]'
cliparse = OptionParser(usage=usage, add_help_option=False)
cliparse.add_option("-d", "--dbname", dest="dbname", action="append",
                    help="databases to monitor to.Can be specified multiple times")
cliparse.add_option("-h", "--host", dest="host", default="127.0.0.1",
                    help="database server host (default: 127.0.0.1)")
cliparse.add_option("-U", "--username", dest="username", default="postgres",
                    help="database user name (default: postgres)")
cliparse.add_option("-p", "--port", dest="port", default="5432",
                    help="database server port (default: 5432)")
cliparse.add_option("-W", "--password", dest="password",
                    help="password to connect to")
cliparse.add_option("--default-database", dest="defaultDatabase", default="postgres",
                    help="default database to connect to monitor Postmaster (default: postgres)")
cliparse.add_option("--diskstat", dest="diskstat", default="postgres", action="store_true",
                    help="display disk cache usage in Performance")
cliparse.add_option("--tupstat", dest="tupstat", default="postgres", action="store_true",
                    help="display tuples r/w in Performance")
cliparse.add_option("--indstat", dest="indstat", default="postgres", action="store_true",
                    help="display index efficiency in Performance")
cliparse.add_option("--help", action="help",
                    help="display this help")
(cliopt, cliargs) = cliparse.parse_args()

if cliopt.password == None:
    cliopt.password = ''

# Connect to default database
curr_user = os.geteuid()
memento = ConfigMemento('last_check', 'check_postgre-' + str(curr_user))
pmconn = None
try:
    pmconn = DBConnection(cliopt.defaultDatabase, cliopt.host, cliopt.port, cliopt.username, cliopt.password)
except Exception as e:
    output.crit(e)
    output.finish()
pm = PgPostmaster(pmconn, memento)
if not pm.isStatEnabled():
    output.unknown('PostgreSQL statistics disabled in config')
    output.finish()

# Connect to user-specified databases
conns = {}
dbs = {}
if cliopt.dbname is not None:
    for i in cliopt.dbname:
        try:
            conns[i] = DBConnection(i, cliopt.host, cliopt.port, cliopt.username, cliopt.password)
        except psycopg2.OperationalError as e:
            output.crit(e)
            output.finish()

        dbs[i] = PgDatabase(conns[i], memento)

# Uptime
message = 'Uptime:' + pm.getUptime() + ' / '

# Queries per second
res = pm.getQps()
message += "queries:%s(%s per second) / " % (res[1], res[2])

# Connection summary
res = pm.getConnSummary()
# [(a,b)(...)] -> "a=b,..."
ipsstr = ','.join(map(lambda x: "%s=%s" % (x[0], x[1]), res['addrs'][0:2]))
usersstr = ','.join(map(lambda x: "%s=%s" % (x[0], x[1]), res['users'][0:2]))
message += "instances:%s / topIPs:%s / topusers:%s" % (res['count'], ipsstr, usersstr)
output.msg(message)

# Fill Performance message

performance = ''

if len(conns):
    pieces = []
    for i in dbs:
        s = []
        if cliopt.diskstat is True:
            res = dbs[i].getDiskCacheInfo()[0]
            s.append("diskread:%s,cachehit:%s(%s%%)" % (res[0][1], res[0][2], int(float(res[0][3]) * 100)))
        if cliopt.tupstat is True:
            res = dbs[i].getTupleLoadTop()
            s.append("tupfetch:%s,tupmod:%s" % (res[0][1], res[0][2]))
        if cliopt.indstat is True:
            res = dbs[i].getTableIndexEfficiencyTop()
            top3 = ','.join(map(lambda x: "%s=seqscan:%s,idxscan:%s(%s%%)" % (x[0], x[1], x[2], x[3]), res[0:2]))
            s.append("TABLES:{" + top3 + "}")
        pieces.append("[%s=" % dbs[i].getDatabaseName() + ','.join(s) + '] ')
    performance += ''.join(pieces)
    output.perf(performance)

memento.saveToFile()

output.finish()
sys.exit(0)

#TODO: slow queries
#TODO: check permission to read statistics
#TODO: WARNING, CRITICAL thresholds in command line