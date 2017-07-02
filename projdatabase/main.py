import os
import struct
from pip._vendor.distlib._backport.tarfile import RECORDSIZE

db_files='data'
root_loc = os.path.dirname(os.path.abspath(__file__))
files_directory= os.path.join(root_loc,db_files)

system_schema = {'schema_name':'information_schema',
                  'schemas_file':'schemata.tbl',
                  'tables_file':'tables.tbl',
                  'columns_file':'columns.tbl'
                  }

tables=[('SCHEMATA',1),('TABLES',3),('COLUMNS',7)]

SCHEMATA = {' SCHEMA_NAME':system_schema['schema_name'],
            'TABLE_ROWS':1,
            'COLUMNS':('')
            }

class StructField:
    '''
    Descriptor representing a simple structure field
    '''
    def __init__(self, format, offset, varchar_size=None):
        self.format = format
        self.offset = offset
    
    def __get__(self, instance, cls):
        if instance is None:
            return self
        else:
            r = struct.unpack_from(self.format,
                                   instance._buffer, self.offset)
            return r[0] if len(r) == 1 else r
    
#     def __set__(self, instance, cls):
#         r = new_file.write(struct.pack_into(self.format,
#                                    instance._buffer, self.offset))
        
        
class Structure:
    def __init__(self, bytedata):
        self._buffer = memoryview(bytedata)
        

'''
Define system tables
'''
class Schemata(Structure):
    schema_name = StructField(format='varchar', offset=0, varchar_size=64)
    
class Tables(Structure):
    table_schema = StructField(format='varchar', offset=0, varchar_size=64)
    table_name = StructField(format='varchar', offset=0, varchar_size=64)
    table_rows = StructField(format='>q', offset=128)

class Columns(Structure):
    table_schema = StructField(format='varchar', offset=0, varchar_size=64)
    table_name = StructField(format='varchar', offset=0, varchar_size=64)
    column_name = StructField(format='varchar', offset=0, varchar_size=32)
    ordinal_position = StructField('>I', 160)
    column_type = StructField(format='varchar', offset=0, varchar_size=64)
    is_nullable = StructField(format='>3s', offset=228)
    column_key = StructField(format='>3s', offset=231)

class DbInstance(object):
    def __init__(self):
        self.db_location = files_directory
        os.chdir(self.db_location)
        self.schema_file_name_long = os.path.join(files_directory, system_schema['schemas_file'])
        self.tables_file_name_long = os.path.join(files_directory, system_schema['tables_file'])
        self.columns_file_name_long = os.path.join(files_directory, system_schema['columns_file'])
        self.schema_file_name_short = system_schema['schemas_file']
        self.tables_file_name_short = system_schema['tables_file']
        self.columns_file_name_short = system_schema['columns_file']
        
    def initialize(self):
        '''
        Initialize DB instance, create and populat 3 system tables:
        Schema file
        Tables file
        Columns file 
        '''
        f = open(self.schema_file_name_short,'wb')
        schema_name=system_schema['schema_name']
        struct_fmt = ">{}p".format(len(schema_name)+1)
        f.write(struct.pack(struct_fmt, bytearray(schema_name,'utf8')))
        f.close()
        
        f = open(self.tables_file_name_short,'wb')
        tables=[('SCHEMATA',1),('TABLES',3),('COLUMNS',7)]
        for table,records_num in tables:
            struct_fmt = "<{}p{}pq".format(len(schema_name)+1,len(table)+1)
            record = (bytearray(schema_name,'utf8'),bytearray(table,'utf8'), int(records_num))
            print(struct_fmt)
            print(record)
            f.write(struct.pack(struct_fmt, *record))
                
        f.close()
        
        f = open(self.columns_file_name_short,'wb')
        columns=[('SCHEMATA','SCHEMA_NAME',1,'varchar(64)','NO',''),
                 ('TABLES','TABLE_SCHEMA',1,'varchar(64)','NO',''),
                 ('TABLES','TABLE_NAME',2,'varchar(64)','NO',''),
                 ('TABLES','TABLE_ROWS',3,'long int','NO',''),
                 ('COLUMNS','TABLE_SCHEMA',1,'varchar(64)','NO',''),
                 ('COLUMNS','TABLE_NAME',2,'varchar(64)','NO',''),
                 ('COLUMNS','COLUMN_NAME',3,'varchar(32)','NO',''),
                 ('COLUMNS','ORDINAL_POSITION',4,'int unsigned','NO',''),
                 ('COLUMNS','COLUMN_TYPE',5,'varchar(64)','NO',''),
                 ('COLUMNS','IS_NULLABLE',6,'varchar(3)','NO',''),
                 ('COLUMNS','COLUMN_KEY',7,'varchar(3)','NO','')]
        
        for table_name,column_name,ord_position,column_type,is_nullable,column_key in columns:
            rec_size = 4+len(schema_name)+1 + len(table_name)+1+len(column_name)+1+len(column_type)+1+len(is_nullable)+1+len(column_key)+1+4
            struct_fmt = "<I{}p{}p{}pI{}p{}p{}p".format(len(schema_name)+1,len(table_name)+1,len(column_name)+1,len(column_type)+1,len(is_nullable)+1,len(column_key)+1)
            record = (int(rec_size),bytearray(schema_name,'utf8'),bytearray(table_name,'utf8'),bytearray(column_name,'utf8'), int(records_num),bytearray(column_type,'utf8'), 
                          bytearray(is_nullable,'utf8'),bytearray(column_key,'utf8'))
            print(struct_fmt)
            print(record)
            f.write(struct.pack(struct_fmt, *record))
                
        f.close()
    
class UserInput(object):
    def __init__(self, prompt=None):
        self.prompt = prompt if prompt is not None else 'davissql>'
        db_instance = DbInstance()
        db_instance.initialize()
        
    def start(self):
        response = input(self.prompt)
        print (response)
        while(response != 'exit'):
            response = input(self.prompt)
            print (response)

if __name__ == '__main__':
    print("Inside Main")
    print(files_directory)
    user_input_inst = UserInput()
    user_input_inst.start()