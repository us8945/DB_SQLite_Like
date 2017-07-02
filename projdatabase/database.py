
import os
import sys
import struct
from struct import Struct
import math

from projdatabase.B_tree import BxTree
#from B_tree import *
from ply import lex, yacc

page_size = 512
page_header_size=8
key_size=6 #4 bytes for key and 2 bytes for offset
payload_init=7 #2 bytes for number of bytes, 4 bytes for key, 1 byte for number of columns 
payload_main=43 #5 - Rowid(INT), 33 - Table_name(TEXT-32), 5 degree(INT)
#tables_dictionary_degree = (page_size-page_header_size)/(key_size+(payload_init+payload_main))
#columns_dictionary_degree = (page_size-page_header_size)/(key_size+(payload_init+payload_main)) 
tables_dictionary_degree = 8
columns_dictionary_degree = 5
null_value='PROJ2-NULL'

db_files='data'
root_loc = os.path.dirname(os.path.abspath(__file__))
files_directory= os.path.join(root_loc,db_files)
tables_dictionary='davisbase_tables'
columns_dictionary='davisbase_columns'

action_dictionary={'create-table'}
types_dictionary={'int':4,
                  'tinyint':1,
                  'smallint':2,
                  'bigint':8,
                  'real':4,
                  'double':8,
                  'datetime':8,
                  'date':8,
                  'text':30}

columns_bytes=  {"null-1":0,
                 "null-2":1,
                 "null-4":2,
                 "null-8":3,
                 'int':6,
                 'tinyint':4,
                 'smallint':5,
                 'bigint':7,
                 'real':8,
                 'double':9,
                 'datetime':10,
                 'date':11,
                 'text':12}

types_dictionary_t={'int':4,
                  'tinyint':1,
                  'smallint':2,
                  'bigint':8,
                  'real':4,
                  'double':8,
                  'datetime':8,
                  'date':8,
                  'text':115}

col_typeTofileFormat={'int':'i',
                  'tinyint':'b',
                  'smallint':'h',
                  'bigint':'q',
                  'real':'f',
                  'double':'d',
                  'datetime':'q',
                  'date':'q',
                  'text':'s'}

nullsFormat=      {'int':2,
                  'tinyint':0,
                  'smallint':1,
                  'bigint':3,
                  'real':2,
                  'double':3,
                  'datetime':3,
                  'date':3,
                  'text':12}

class DbInstance(object):
    def __init__(self):
        self.db_location = files_directory
        os.chdir(self.db_location)
        self.db_new=True
        if os.path.exists(tables_dictionary+".tbl"):
            self.db_new=False
            #print("DB exists")
        self.tables_file_name_short = tables_dictionary+'.tbl'
        self.columns_file_name_short = columns_dictionary+'.tbl'
        #self.tables_dict = BxTree(degree=tables_dictionary_degree,db_file_name=self.tables_file_name_short)
        #self.columns_dict = BxTree(degree=columns_dictionary_degree,db_file_name=self.columns_file_name_short)
        self.tables_dict=None
        self.columns_dict=None
        if self.db_new==True:
            print("initialize DB")
            self._create_table_dict(tables_dictionary)
            self._create_table_dict(columns_dictionary)
            dictionary_columns=[('rowid', 'int', 'not-null'), ('table_name', 'text', 'not-null')]
            self._create_columns_dict(tables_dictionary, dictionary_columns)
            dictionary_columns=[('rowid', 'int', 'not-null'), ('table_name', 'text', 'not-null'),
                                ('column_name', 'text', 'not-null'),('data_type','text','not-null'),
                                ('ordinal_position','tinyint','not-null'),('is_nullable','text','not-null')]
            
    
            self._create_columns_dict(columns_dictionary, dictionary_columns)
        
    def _create_table_dict(self,table_name): 
        '''TODO: store new table degree in dictionary
        '''
        #print('Table name:..'+table_name+" , Table len:.."+str(len(table_name)))
        if self.tables_dict is None:
            self.tables_dict = BxTree(degree=tables_dictionary_degree,db_file_name=self.tables_file_name_short)
        last_rowid=self.tables_dict.root.numberOfKeys   
        text_byte=0x0C+len(table_name)
        payload_fmt=">HIBB{}s".format(len(table_name))
        #print(payload_fmt)
        payload_struct=Struct(payload_fmt)
        payload=(int(payload_struct.size),int(last_rowid+1),int(1),int(text_byte),table_name.encode('utf-8'))
        status=self.tables_dict.insert(key=last_rowid+1, payload=payload_struct.pack(*payload))
        self.tables_dict=None
        return status
    
    def _create_columns_dict(self,table_name,columns):
        '''
        columns fields:
             TABLE_NAME       
             COLUMN_NAME       
             DATA_TYPE
             ORDINAL_POSITION         
             IS_NULLABLE              
         '''
        #print("Create columns table dictionary..."+table_name)
        if self.columns_dict is None:
            self.columns_dict = BxTree(degree=columns_dictionary_degree,db_file_name=self.columns_file_name_short)
        ordinal_pos=1
        last_rowid=self.columns_dict.record_count()
        for column in columns:
            #last_rowid=self.columns_dict.root.numberOfKeys
            #print("Rowid.."+str(last_rowid))
            #print('Columns',column)
            column_name=column[0]
            column_type=column[1]
            if column[2]==None:
                column_null='YES'   
            elif column[2] in (['not-null'],['primary-key'],'not-null'):
                column_null='NO'
            
            table_name_byte=0x0C+len(table_name)
            column_name_byte=0x0C+len(column_name)
            column_type_byte=0x0C+len(column_type)
            column_null_byte=0x0C+len(column_null)
            payload_fmt=">HIBBBBBB{}s{}s{}sB{}s".format(len(table_name),len(column_name),len(column_type),len(column_null))
            #                   |          table name
            #                       |           column name
            #print(payload_fmt)
            payload_struct=Struct(payload_fmt)
            payload=(int(payload_struct.size),int(last_rowid+1),int(5),int(table_name_byte),int(column_name_byte),int(column_type_byte),
                     int(4),int(column_null_byte),table_name.encode('utf-8'),
                     column_name.encode('utf-8'),column_type.encode('utf-8'),
                     int(ordinal_pos), column_null.encode('utf-8'))
                     
            #print("Payload before calling node insert"+str(payload)+str(payload_struct.pack(*payload)))
            status=self.columns_dict.insert(key=last_rowid+1, payload=payload_struct.pack(*payload))
            ordinal_pos+=1
            last_rowid+=1
        self.columns_dict=None
        return status
    
    def _calculate_degree(self,columns):
        '''
        page_size = 512
        page_header_size=8
        key_size=6 #4 bytes for key and 2 bytes for offset
        payload_init=7 #2 bytes for number of bytes, 4 bytes for key, 1 byte for number of columns 
        payload_main=43 #5 - Rowid(INT), 33 - Table_name(TEXT-32), 5 degree(INT)
        #tables_dictionary_degree = (page_size-page_header_size)/(key_size+(payload_init+payload_main))
        #columns_dictionary_degree = (page_size-page_header_size)/(key_size+(payload_init+payload_main)) 
        '''
        record_size=7 #Initial record size:record length, key and number of columns 
        #print('Degree columns',columns)
        for column in columns:
            record_size+=(types_dictionary[column[1]]+1)
            #print('Loop record size',record_size)
        degree=int((512-8)/record_size)
        #print('Calculate degree, columns',columns)
        #print('Degree',degree)
        return degree
    
    def get_table_degree(self,table_name):
        table_definition=self.get_columns_definition(table_name)
        #print('degree calc, table def',table_definition)
        columns=[]
        for key in sorted(table_definition):
            if table_definition[key]!=[]:
                column=(table_definition[key][1],table_definition[key][2],None)
                columns.append(column)
        #print('columns',columns)
        degree=self._calculate_degree(columns)
        #print("Calculated degree",degree)
        return degree
    
    def create_table(self,table_name,columns):
        if os.path.exists(table_name+".tbl"):
            print("Table Already exists")
            return "Table Already exists"
        self._create_table_dict(table_name)
        self._create_columns_dict(table_name, columns)
        table_degree=self._calculate_degree(columns)
        new_table=BxTree(degree=table_degree,db_file_name=table_name+'.tbl')
        del new_table 
        return 'Success'  
    
    def format_records_dictionary(self,rec_dict,fields=None):
        ''' Printing output to screen...don't comment out
        '''
        if fields is not None:
            print (fields)
        for key in sorted(rec_dict):
            print(key,'      ',rec_dict[key])
        return
    
    def get_columns_definition(self,table_name):
        if self.columns_dict is None:
                self.columns_dict = BxTree(degree=columns_dictionary_degree,db_file_name=self.columns_file_name_short)
        records=self.columns_dict.select_from_table(table_name, columns=None, where_condition=None)
        return_rec={}
        #print('Get Col def recs',records)
        for key in sorted(records):
            if records[key] != []:
                if (records[key][0].decode('utf-8')==table_name):
                    return_rec[key]=records[key]  
                              
        for key in sorted(return_rec):
            return_rec[key]=[return_rec[key][0].decode('utf-8'),return_rec[key][1].decode('utf-8'),return_rec[key][2].decode('utf-8'),
                                  return_rec[key][3],return_rec[key][4].decode('utf-8')]
        #print ('return_records table definition',return_rec)
        return return_rec
    
    def set_proper_type(self,table_name,field_name,field_value):
        table_definition=self.get_columns_definition(table_name)
        return_val=field_value
        #print('Table definition',table_definition)
        #print('Inputs for type',field_name)
        for key in sorted(table_definition):
            if table_definition[key][1]==field_name:
                field_type=table_definition[key][2]
                ordinal_position=table_definition[key][3]
        if field_type in ('int','tynyint','smallint','bigint'):
            return_val=int(field_value)
        if field_type in ('date','datetime'):
            field_value=field_value[1:-1]
            return_val=int(field_value.replace('-',''))
        if field_type in ('text'):
            return_val=field_value[1:-1]
        elif field_type in ('real','double'):
            return_val=float(field_value)
        
        #print('Return converted..',return_val,ordinal_position)
        return return_val,ordinal_position
     
    def get_field_names(self,table_name):
        table_definition=self.get_columns_definition(table_name)
        #print('table_definition',table_definition)
        fields_for_print=[]
        for key in sorted(table_definition):
            fields_for_print.append(table_definition[key][1])
        
        return fields_for_print
        
    
    def filter_records(self,records,where_condition):
        if not(where_condition[1]=='rowid' or where_condition[1]=='row_id'):
            print("Only rowid is supported in WHERE clause")
            return None
        compare_value=where_condition[2][1]
        compare_operator=where_condition[0]
        return_records={}
        if compare_operator=='=':
            return_records[compare_value]=records[compare_value]
            return return_records
        else:
            for key in sorted(records):
                comp_exp=str(key)+str(compare_operator)+str(compare_value)
                #print("Comp exp",comp_exp)
                exp_res=eval(comp_exp)
                if exp_res:
                    return_records[key]=records[key]
        return return_records
        
    def select_table(self,table_name,columns,where_condition):
        #print("In Select")
        table_degree=4 # default to 4 for select
        if not(os.path.exists(table_name+".tbl")):
            print ("Table doesn't exists")
            return
        if table_name=='davisbase_tables':
            if self.tables_dict is None:
                self.tables_dict = BxTree(degree=tables_dictionary_degree,db_file_name=self.tables_file_name_short)
            select_table=self.tables_dict
        elif table_name=='davisbase_columns':
            if self.columns_dict is None:
                self.columns_dict = BxTree(degree=columns_dictionary_degree,db_file_name=self.columns_file_name_short)
            select_table=self.columns_dict
        else:
            #select_file=open(table_name+".tbl",'r+b')
            select_table=BxTree(degree=table_degree,db_file_name=table_name+".tbl")

        records=select_table.select_from_table(table_name, columns, where_condition)
        field_names=self.get_field_names(table_name)
        if where_condition is not None:
            records=self.filter_records(records,where_condition)
        if records is not None:
            self.format_records_dictionary(records,field_names)
        return 'Success'
    
    def drop_table(self,table_name):
        print("Dropping  table..",table_name)
        if not(os.path.exists(table_name+".tbl")):
            print ("Table doesn't exists")
            return

        if table_name=='davisbase_tables' or table_name=='davisbase_columns':
            print('Cannot drop dictionary table')
            return
        else:
            os.remove(table_name+".tbl")
            print ("Table dropped")
        return
    
    def update_table(self,table_name,input_from_parser):
        #print("In Update")
        rowid_key=input_from_parser[0][3][2][1]
        where_action=input_from_parser[0][3][0]
        update_fld_value=input_from_parser[0][2]
        if where_action!='=':
            print("Only EQUAL supported in WHERE")
            return

        table_degree=4 # default to 4 for select
        if not(os.path.exists(table_name+".tbl")):
            print ("Table doesn't exists")
            return

        if table_name=='davisbase_tables' or table_name=='davisbase_columns':
            print('Cannot update dictionary table, use DDL instead')
            return
        else:
            update_table=BxTree(degree=table_degree,db_file_name=table_name+".tbl")
        
        field_name=input_from_parser[0][2][0][0]
        val=input_from_parser[0][2][0][1]
        field_value=val[8:val.find(')')] #still string
        field_val,update_fld_ord_pos=self.set_proper_type(table_name,field_name,field_value)
        orig_record=update_table._get_record_by_rowid(rowid_key)
        if orig_record=='KEY_NOT_FOUND':
            return
        #print('Update inputs',orig_record,update_fld_ord_pos,field_val)
        update_table.update_record_byoffset(orig_record[rowid_key],update_fld_ord_pos,field_val,rowid_key)
        return
    
    def format_payload(self,table_columns,insert_values):
        #col_typeTofileFormat
        struct_format_dic={}
        #print('insert_values',insert_values)
        insert_values=insert_values[0][len(insert_values[0])-1]
        #print('insert_values',insert_values)
        ii=[]
        for ins in insert_values:
            ii.append(ins[1])
        insert_values=ii
        
        not_null_violated=False
        #print('insert_values',insert_values)
        #print('table_columns',table_columns)
        for key in sorted(table_columns):
            #Get column type and its struct format. Example, for "int" get "i"
            ord_position=int(table_columns[key][3])
            if table_columns[key][4]=='NO': #check for null in input
                if insert_values[ord_position-1]=='PROJ2-NULL':
                    not_null_violated=True          
            table_data_type=table_columns[key][2]
            struct_format=col_typeTofileFormat[table_data_type] #Get column type and its struct format
            null_format=nullsFormat[table_data_type] #Get null byte for column type. Example, for "int" get 2
            bytes_format=columns_bytes[table_data_type]
            ord_position=int(table_columns[key][3])
            if struct_format=='s':
                struct_format=str(len(insert_values[ord_position-1]))+'s' 
                bytes_format=bytes_format+len(insert_values[ord_position-1])          
            struct_format_dic[ord_position]=(table_data_type,struct_format,null_format,bytes_format)
            
        #print('struct_format_dic',struct_format_dic)
        #print('insert_values',insert_values)
        
        data_format=''
        data_list=[]
        bytes_header_format=''
        bytes_header_list=[]
        header_format='>HiB' #Size, ROWID-KEY, Num-Columns
        header_list=[0,int(insert_values[0]),(len(insert_values)-1)] #Update late first item with record size
        
        for index,val in enumerate(insert_values):
            if index!=0: #Skip key
                if val==null_value:
                    bytes_header_list.append(struct_format_dic[index+1][2])
                    bytes_header_format=bytes_header_format+'B'
                else:
                    if isinstance(val, str):
                        if struct_format_dic[index+1][0]=='date' or struct_format_dic[index+1][0]=='datetime':
                            data_list.append(int(val.replace('-','')))
                        else:
                            data_list.append(val.encode('utf-8'))
                    else:
                        data_list.append(val)
                    bytes_header_format=bytes_header_format+'B'
                    data_format = data_format + struct_format_dic[index+1][1]
                    bytes_header_list.append(struct_format_dic[index+1][3])
        
        full_rec_format=header_format+bytes_header_format+data_format
        ful_rec_struct = Struct(full_rec_format)
        header_list[0]=ful_rec_struct.size
        
        #print('full_rec_format',full_rec_format)
        record_tuple=tuple(header_list+bytes_header_list+data_list)
        #print('record_tuple',record_tuple)
        #print('full_rec_format',full_rec_format)
        #print('payload',ful_rec_struct.pack(*record_tuple))
        return insert_values[0],ful_rec_struct.pack(*record_tuple),not_null_violated
        
    def insert_into_table(self,table_name,insert_values):
        if (table_name=='davisbase_tables' or table_name=='davisbase_columns'):
            print ("Cannot insert to dictionary tables. Use create table instead")
            return
        min_table_degree=4 #TODO to change calculation based on dictionary
        table_degree=max(self.get_table_degree(table_name),min_table_degree)
        #print('table_degree',table_degree)
        #insert_file=open(table_name+".tbl",'r+b')
        insert_table=BxTree(degree=table_degree,db_file_name=table_name+".tbl")
        table_columns=self.get_columns_definition(table_name)
        #print(table_columns)
        key,payload,not_null_violated=self.format_payload(table_columns,insert_values)
        if not_null_violated:
            print('NOT NULL Constraint violated, cannot insert NULL into NOT NULL FIELD')
            return
        status=insert_table.insert(key, payload)
        #print(payload)
        #status=self.tables_dict.insert(insert_values[0], payload)
        #print(status)
        return status
    
    def run_action(self,result):
        #print("Run result")
        if result[0][0]=='create-table':
            table_name=result[0][1]
            columns=result[0][2]
            if not (columns[0][1] =='int' and (columns[0][0]=='rowid' or columns[0][0]=='row_id')):
                print('First column should be ROWID/ROW_ID and INTEGER')
                return 'First column should be ROWID and INTEGER'
            return self.create_table(table_name,columns)
        
        elif result[0][0]=='select':
            table_name=result[0][2]
            columns=result[0][1]
            where_condition=result[0][3]
            return self.select_table(table_name,columns,where_condition)
        elif result[0][0]=='insert':
            table_name=result[0][1]
            insert_values=result
            status=self.insert_into_table(table_name,insert_values)
            return
        elif result[0]=='show-tables':
            table_name='davisbase_tables'
            columns='*'
            where_condition=None
            return self.select_table(table_name,columns,where_condition)
        elif result[0][0]=='update':
            table_name=result[0][1]
            if len(result[0])<4:
                print("Specify WHERE condition, failed")
                return
            return self.update_table(table_name,result)
        elif result[0][0]=='drop-table':
            table_name=result[0][1]
            return self.drop_table(table_name)
            

from projdatabase.sqlparser import *
                
if __name__ == '__main__':
    #print("Inside Main")
    print(files_directory)
    p = SqlParser()
    p.build()
    lexer = SqlLexer().build()
    db_instance = DbInstance()
    while True:
        text = input("davisql> ").strip()
        line=text.split(";")
        if (len(line)>1):
            for l in line:
                if l.lower() in ["quit","exit","exit;"]:
                    break
                if l:
                    try:
                        result = p.text(l)
                        #print("parse result -> %s" % result)
                        if result==None:
                            pass
                        else:
                            #print("Run action")
                            db_instance.run_action(result)
                    except:
                        print("Invalid Input")
                        print(l)
                        e = sys.exc_info()[0]
                        print("Error",e)
        else:
            if text.lower() in ["quit","exit","exit;"]:
                break
            if text:
                result = p.text(text)
                #print("parse result -> %s" % result)
                if result==None:
                    pass
                else:
                    db_instance.run_action(result)
    
    #interactive()

    