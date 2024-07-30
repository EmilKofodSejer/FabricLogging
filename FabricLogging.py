from notebookutils import mssparkutils
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType
import json
import csv
from datetime import datetime



CRITICAL = 50
ERROR = 40
WARNING = 30
INFO = 20
DEBUG = 10
NOTSET = 0

_levelToName = {
    CRITICAL: 'CRITICAL',
    ERROR: 'ERROR',
    WARNING: 'WARNING',
    INFO: 'INFO',
    DEBUG: 'DEBUG',
    NOTSET: 'NOTSET',
}
_nameToLevel = {
    'CRITICAL': CRITICAL,
    'ERROR': ERROR,
    'WARNING': WARNING,
    'INFO': INFO,
    'DEBUG': DEBUG,
    'NOTSET': NOTSET,
}

def _checkLevel(level):
    if isinstance(level, int):
        rv = level
    elif str(level) == level:
        if level not in _nameToLevel:
            raise ValueError("Unknown level: %r" % level)
        rv = _nameToLevel[level]
    else:
        raise TypeError("Level not an integer or a valid string: %r"
                        % (level,))
    return rv

def getLevelName(level):
    result = _levelToName.get(level)
    if result is not None:
        return result
    result = _nameToLevel.get(level)
    if result is not None:
        return result
    return "Level %s" % level

class Filter():
    def __init__(self, name):
        self.name = name

class FabricLogger():

    def __init__(self, name, level=NOTSET, handlers = [], **kwargs):
        if not isinstance(name, str):
            raise TypeError('A logger name must be a string')
        
        self.name = name
        self.level = _checkLevel(level)
        self.disabled = False
        self.handlers = handlers


    def isEnabledFor(self, level):
        if self.disabled:
            return False

        if level >= self.level:
            return True

    def addHandler(self, handler):
        self.handlers.append(handler)

    def debug(self, msg):
        if self.isEnabledFor(DEBUG):
            self._log(DEBUG, msg)

    def info(self, msg):
        if self.isEnabledFor(INFO):
            self._log(INFO, msg)
    
    def warning(self, msg):
        if self.isEnabledFor(WARNING):
            self._log(WARNING, msg)

    def error(self, msg):
        if self.isEnabledFor(ERROR):
            self._log(ERROR, msg)

    def critical(self, msg):
        if self.isEnabledFor(CRITICAL):
            self._log(CRITICAL, msg)

    def _log(self, level, msg, **kwargs):

        record = self.makeRecord(self.name, level, msg, **kwargs)
        self.handle(record)

    def makeRecord(self, name, level, message, **kwargs):
        if kwargs is not None:
            for key in kwargs:
                if (key in ["name","message","level","asctime"]):
                    raise KeyError("Attempt to overwrite %r in LogRecord" % key)

        rv = LogRecord(name, level, message)

        return rv
    
    def handle(self, record):

        if self.disabled:
            return

        #if isinstance(maybe_record, LogRecord):
        #    record = maybe_record
        self.callHandlers(record)

    def callHandlers(self, record):
        c = self
        #while c:
        for hdlr in c.handlers:
            if record.levelnumber >= hdlr.level:
                hdlr.handle(record)
                
class LogRecord:

    def __init__(self, name, level, message, **kwargs):
        self.name = name
        self.message = message
        self.levelname = getLevelName(level)
        self.levelnumber = _checkLevel(level)
        self.kwargs = kwargs

    def return_as_dict(self):
        LogRecord_as_dict = {
            'name': {"value":self.name, "dtype":"<class 'str'>"},
            'message': {"value":self.message, "dtype":"<class 'str'>"},
            'levelname': {"value":self.levelname, "dtype":"<class 'str'>"},
            'levelnumber': {"value":self.levelnumber, "dtype":"<class 'int'>"},
        }
        LogRecord_as_dict.update({str(key):{value:v, dtype: str(type(v))} for (key,v) in self.kwargs.items()})

        return LogRecord_as_dict
    
    #def return_as_table(self): 
    #    mssparkutils.fs.put(self.full_path, "")

    def list_schema(self):
        return self.return_as_dict().keys()

    def list_values(self):
        return [d["value"] for d in self.return_as_dict().values()]

class CSVHandler:

    def __init__(self, handler_name, file_name = "", path = "", delta_table = "",level=NOTSET):
        self.level = level
        self.handler_name = handler_name

        if not file_name:
            self.file_name = f"/{handler_name}_{datetime.today().strftime('%Y-%m-%d')}.csv"
        else:
            self.file_name = file_name

        if not path:
            self.path = f"Files/LogHandler/{self.handler_name}"
        else:
            self.path = path

        self.full_path = self.path + self.file_name

        if not delta_table:
            self.delta_table = self.handler_name
        else:
            self.delta_table = delta_table

    def handle(self, record):
        rv = record
        if isinstance(rv, LogRecord):
            record = rv
        if rv:
            self.emit(record)
        return rv

    def check_if_log_file_exists(self, full_path):     
        return mssparkutils.fs.exists(full_path)

    def emit(self, record):
        if not self.check_if_log_file_exists(self.full_path):
            mssparkutils.fs.put(self.full_path, self.create_csv_header(record.list_schema())) 

        mssparkutils.fs.append(self.full_path, """\n""" + str(record.list_values()), True)

    def create_csv_header(self, record_headers,delimiter = ","):
        return delimiter.join(record_headers)

    def commit_csv_to_delta(self, fiilter=""):
        
        if not self.check_if_log_file_exists(self.full_path):
            raise Exception("Cannot find CSV to commit.")

        df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(self.full_path)
        df.write.format("delta").mode("overwrite").save(f"Tables/{self.delta_table}")
        
class StructuredStreamHandler():

    def __init__(self, abfs_path):
        self.abfs_path = abfs_path


