from notebookutils import mssparkutils
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType
import json
import csv

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

_datatypeConversions = {
    "<class 'int'>" : IntegerType(),
    "<class 'float'>" : FloatType(),  
    "<class 'str'>" : StringType(),
    "<class 'bool'>" : BooleanType(),
    "<class 'list'>" : StringType(),
    "<class 'dict'>" : StringType()
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

class HandlerBase:

    def __init__(self, tablename, level=NOTSET, abfs_path='',  schema_evolution_enabled=True):
        self._name = None
        self.level = _checkLevel(level)
        self.abfs_path = abfs_path
        self.schema_evolution_enabled = schema_evolution_enabled
        
    
    def setLevel(self, level):
        self.level = _checkLevel(level)

    def handle(self, record):
        rv = record
        if isinstance(rv, LogRecord):
            record = rv
        if rv:
            self.emit(record)
        return rv
    
    def emit(self, record):
        raise NotImplementedError('emit must be implemented '
                                  'by Handler subclasses')

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
            #if not c.propagate:
            #    c = None    #break out


class LogRecord:

    def __init__(self, name, level, message, **kwargs):
        #ct = time.time_ns()
        self.name = name
        self.message = message
        self.levelname = getLevelName(level)
        self.levelnumber = _checkLevel(level)
        self.kwargs = kwargs

    def return_as_dict(self):
        LogRecord_as_dict = {
            'name': self.name,
            'message': self.message,
            'levelname': self.levelname,
            'levelnumber': self.levelnumber
        }
        LogRecord_as_dict.update(self.kwargs)

        return LogRecord_as_dict
    
    def list_schema(self):
        schema = self.return_as_dict().keys()
        return list(schema)

    def list_values(self):
        values = self.return_as_dict().values()
        return list(values)

    def infer_LogRecord_schema(self):
        variable = 1

class DeltaTableHandler(HandlerBase):

    def __init__(self, tablename):
        self.tablename = tablename
        self.full_path = abfs_path + "/" + tablename


    def delta_table_exists(self, path, tablename):
        files = mssparkutils.fs.ls(path)
        for file in files:
            if file.name == tablename:
                return True

        return False

    def get_delta_table_schema_names(self, path):
        df = spark.read.format("delta").load(path)
        schema = df.schema
        field_names = [field.name for field in schema.fields]
        return field_names

    def get_delta_table_schema(self, path):
        df = spark.read.format("delta").load(path)
        schema = df.schema

        return schema

    def schema_compare(self, record, delta_schema):
        return record.list_schema() != delta_schema

    def emit(self, record):
        
        # Creates a delta table if it does not exist
        if not self.delta_table_exists(self.abfs_path, self.tablename):
            schema = StructType([
                StructField(f"{column}", StringType(), True) if column != "levelnumber" else StructField(f"{column}", IntegerType(), True) for column in record.list_schema() 
            ])

            df = spark.createDataFrame(data = [], schema = schema)
            df.write.format("delta").mode("overwrite").save(self.full_path)

        df_to_insert = spark.createDataFrame(data=[record.list_values()], schema=self.get_delta_table_schema(self.full_path))
        
        print(self.schema_compare(record, self.get_delta_table_schema_names(self.full_path)))
        #Checks if the schema of the record matches the schema of the delta table
        if self.schema_compare(record, self.get_delta_table_schema_names(self.full_path)): 
            #If schema evolution is allowed for the table it will evolutionize the schema and insert the data
            if self.schema_evolution_enabled: 
                df_to_insert.write.format("delta").mode("append").option("mergeSchema", "true").save(self.full_path)
            else: 
                raise Exception(f"Schema evolution necessary but was set to {self.schema_evolution_enabled}")
        else:
            print(self.get_delta_table_schema(self.full_path))
            print(df_to_insert.schema)
            df_to_insert.write.format("delta").mode("append").save(self.full_path)

class CSVHandler(HandlerBase):

    def __init__(self, file_name, mode, abfs_path, level=NOTSET):
        self.level = level
        self.file_name = file_name
        self.mode = mode
        self.abfs_path = abfs_path
        self.full_path = self.abfs_path + "/" + self.file_name
    
    def check_if_log_file_exists(self, full_path):     
        return mssparkutils.fs.exists(full_path)

    def emit(self, record):
        if not self.check_if_log_file_exists(self.full_path):
            mssparkutils.fs.put(self.full_path, "")
            
        mssparkutils.fs.append(self.full_path, """\n""" + str(record.list_values()), True)

    def commmit_to_delta(self, filter):
        variable = 1
    

class StructuredStreamHandler(HandlerBase):

    def __init__(self, abfs_path):
        self.abfs_path = abfs_path
