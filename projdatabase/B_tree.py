'''
Used below source as starting template. The template was for regular Btree
Source: http://knuth.luther.edu/~leekent/CS2Plus/chap10/chap10.html
Full Tempalte is under package btree_template
'''
#os.chdir("C://Users/uri/Google Drive/UTD/Summer_2016/CS_6360_DB/Project_2/code/projdatabase")
import datetime
import os
from copy import deepcopy
import sys
import math
import struct
from struct import Struct
import projdatabase.leaf_node
import projdatabase.inner_node
from projdatabase.inner_node import InnerNode
from projdatabase.leaf_node import LeafNode

page_size=512
leaf_page=0x0d
inner_page=0x05
page_fbyte_fmt=">b"
page_fbyte_struct=Struct(page_fbyte_fmt)

leaf_page_header_fmt=">bbH"
leaf_page_header_struct=Struct(leaf_page_header_fmt)

inner_page_header_fmt=">bbHI"
inner_page_header_struct=Struct(inner_page_header_fmt)

record_header_fmt='>HiB'
record_header_struct=Struct(record_header_fmt)

record_columns_fmt='>B'
record_columns_struct=Struct(record_columns_fmt)

db_files='data'
tables_dictionary='davisbase_tables.tbl'
columns_dictionary='davisbase_columns.tbl'
root_loc = os.path.dirname(os.path.abspath(__file__))
files_directory= os.path.join(root_loc,db_files)

types_to_format={0:"NULL",
                 1:"NULL",
                 2:"NULL",
                 3:"NULL",
                 4:"b",
                 5:"h",
                 6:"i",
                 7:"q",
                 8:"f",
                 9:"d",
                 10:"Q",
                 11:"Q",}



class BxTree:
    def __init__(self, degree,db_file_name,table_name=None,dic_file=None):
        self.db_location = files_directory
        os.chdir(self.db_location)
        self.db_file_name = db_file_name
        self.degree = degree
        self.one_page_tree=False
        self.table_name=table_name
        if os.path.exists(self.db_file_name):
            if dic_file is not None:
                self.file=dic_file
            else:
                self.file = open(self.db_file_name,'r+b')
                self.root=self._load_root()
        else:
            self.file = open(self.db_file_name,'w+b')
            self.create_leaf_page(1)
            self.root= LeafNode(degree=self.degree,page=1,file=self.file)
            self.one_page_tree=True
        self.num_pages = self.get_num_pages(self.file)
        
    def __del__(self):
        self.file.close()
        
    def __repr__(self):
        # This method is complete
        return "BTree("+str(self.degree)+", Root= "+repr(self.root)+", Num Pages ="+str(self.num_pages)+")"
    
    def get_num_pages(self,file_input):
        file=file_input
        old_file_position = file.tell()
        file.seek(0, os.SEEK_END)
        size = file.tell()
        #print("File size is "+str(size))
        file.seek(old_file_position, os.SEEK_SET)
        return math.ceil(size/page_size)
    
    def create_leaf_page(self,page):
        self.file.seek((page-1)*page_size)
        self.file.write(bytearray(page_size))
        self.file.seek((page-1)*page_size)
        header=(leaf_page,0,page_size)
        self.file.write(leaf_page_header_struct.pack(*header))
        
    def create_inner_page(self,page):
        self.file.seek((page-1)*page_size)
        self.file.write(bytearray(page_size))
        self.file.seek((page-1)*page_size)
        #self.file.write(bytearray(page_size))
        header=(inner_page,0,page_size,0)
        self.file.write(inner_page_header_struct.pack(*header))
        
    def table_record_format(self):
        record_dictionary=self._get_record_dictionary()
        print(record_dictionary)
    
    def select_from_table(self,table_name,columns=None,where_condition=None):
        record_dictionary=self._get_all_records()
        return record_dictionary
        
    def _read_unpack_record(self):
        record_header=record_header_struct.unpack(self.file.read(record_header_struct.size))
        record_key=record_header[1]
        record_size=record_header[0]
        record_columns=record_header[2]
        column_fmt='>'
        columns_val=[]
        column_byte_format_full=[]
        if record_size>record_header_struct.size:
            #print("Record number of columns..",record_columns)
            #column_fmt='>'
            #columns_val=[]
            for col in range(0,record_columns):
                column_byte_format=record_columns_struct.unpack(self.file.read(record_columns_struct.size))[0]
                column_byte_format_full.append(column_byte_format)
                #print(column_byte_format)
                if (column_byte_format >= 12):
                    str_length=column_byte_format-12
                    str_format=str(str_length)+'s'
                    #print(str_format,type(str_format))
                    column_fmt=''.join((column_fmt,str_format))
                    columns_val.append('value')
                else:
                    column_pyth_format=types_to_format[column_byte_format]
                    if (column_pyth_format != 'NULL'):
                        column_fmt=''.join((column_fmt,column_pyth_format))
                        columns_val.append('value')
                    else:
                        columns_val.append('NULL')    
        #print("Record format..",column_fmt)                        
        record_struct=Struct(column_fmt)
        record=record_struct.unpack(self.file.read(record_struct.size))
        #print("Read record",record)
        #print("Check for Nulls..",columns_val)
        record_final=[]
        index=0
        for col in columns_val:
            if col=='value':
                record_final.append(record[index])
                index+=1
            else:
                record_final.append('NULL')
        
        full_col_fmt=record_header_fmt+('B'*record_columns)+column_fmt[1:]
        
        return record_key,record_final,full_col_fmt,column_byte_format_full
    
    def _get_all_records(self):
        self.file.seek(0)
        num_pages=self.get_num_pages(self.file)
        #print("Dictionary num pages are..",num_pages)
        records={}
        for page in range(1,num_pages+1):
            self.file.seek((page-1)*page_size)
            page_header=leaf_page_header_struct.unpack(self.file.read(leaf_page_header_struct.size))
            #print("Page header is..",page,page_header,self.file.tell())
            if page_header[0]==leaf_page:
                #print("Found leaf node on page..",page)
                num_records=page_header[1]
                #print("Leaf number of records is ",num_records)
                self.file.seek((page-1)*page_size+page_header[2])
                for rec in range(0,num_records):
                    record_key,unpacked_record,col_fm,col_bt_fmt = self._read_unpack_record()
                    records[record_key]=unpacked_record
        #print("Return records dictionary",records)                                       
        return records
    
    def _get_record_by_rowid(self,rowid_key):
        self.file.seek(0)
        num_pages=self.get_num_pages(self.file)
        #print("Dictionary num pages are..",num_pages)
        record={}
        for page in range(1,num_pages+1):
            self.file.seek((page-1)*page_size)
            page_header=leaf_page_header_struct.unpack(self.file.read(leaf_page_header_struct.size))
            #print("Page header is..",page,page_header,self.file.tell())
            if page_header[0]==leaf_page:
                #print("Found leaf node on page..",page)
                num_records=page_header[1]
                #print("Leaf number of records is ",num_records)
                self.file.seek((page-1)*page_size+page_header[2])
                for rec in range(0,num_records):
                    file_offset=self.file.tell()
                    record_key,unpacked_record,col_fm,col_bt_fmt = self._read_unpack_record()
                    if record_key==rowid_key:
                        record[record_key]=(file_offset,unpacked_record,col_fm,col_bt_fmt)
                        return record
        print('Key not found, Update failed')
        return 'KEY_NOT_FOUND'                                
        
        
    def _get_record_dictionary(self):
        self.column_dic_file.seek(0)
        num_pages=self.get_num_pages(self.column_dic_file)
        #print("Dictionary num pages are..",num_pages)
        records={}
        for page in range(1,num_pages+1):
            self.column_dic_file.seek((page-1)*page_size)
            page_header=leaf_page_header_struct.unpack(self.column_dic_file.read(leaf_page_header_struct.size))
            #print("Page header is..",page,page_header,self.column_dic_file.tell())
            if page_header[0]==leaf_page:
                #print("Found leaf node on page..",page)
                num_records=page_header[1]
                #print("Leaf number of records is ",num_records)
                self.column_dic_file.seek((page-1)*page_size+page_header[2])
                for rec in range(0,num_records):
                    record_key,unpacked_record,col_fm,col_bt_fmt = self._read_unpack_record()
                    records[record_key]=unpacked_record
        #print("Return records dictionary",records)                                       
        return records
        
    def _load_root(self):
        ''' Returns Root Node'''
        self.file.seek(0)
        page_header=leaf_page_header_struct.unpack(self.file.read(leaf_page_header_struct.size))
        if page_header[0]==leaf_page:
            root = LeafNode(degree=self.degree,page=1,file=self.file)
            self.one_page_tree=True
        else:
            root = InnerNode(degree=self.degree,page=1,file=self.file)
            
        return root    
    
    def update_record_byoffset(self,orig_record,update_fld_ord_pos,field_val,key):
        update_key=key
        update_rec=orig_record[1]
        #print('Update rec,',update_rec)
        file_offset=orig_record[0]
        rec_fmt=orig_record[2]
        rec_fld_bytes=orig_record[3]
        if type(field_val)==str:
            update_rec[update_fld_ord_pos-2]=field_val.encode('utf-8')
        else:
            update_rec[update_fld_ord_pos-2]=field_val
        rec_struct=Struct(rec_fmt)
        update_rec_arr=[]
        update_rec_arr.append(rec_struct.size)
        update_rec_arr.append(update_key)
        update_rec_arr.append(len(update_rec))
        for f in rec_fld_bytes:
            update_rec_arr.append(f)
        for f in update_rec:
            update_rec_arr.append(f)
        
        update_rec_tuple=tuple(update_rec_arr)
        #print('Update record tuple',update_rec_tuple)
        self.file.seek(file_offset)
        self.file.write(rec_struct.pack(*update_rec_tuple))
        
        #print('Update record array',update_rec_arr)
        return
            
    
    def record_count(self):
        record_count=0
        for page in range(0,self.num_pages):
            self.file.seek(page*page_size)
            page_header=leaf_page_header_struct.unpack(self.file.read(leaf_page_header_struct.size))
            if page_header[0]==leaf_page:
                record_count+=page_header[1]
            
        return record_count
        
            
    def _find_node(self,key,starting_node):
        ''' Recursive function to find node with the key or to insert key
            Returns Leaf node
        '''
        page = starting_node.searchNode(key)
        #print("Page in find node.",page)
        self.file.seek((page-1)*page_size)
        page_header=leaf_page_header_struct.unpack(self.file.read(leaf_page_header_struct.size))
        if page_header[0]==leaf_page:
            return LeafNode(degree=self.degree,page=page,file=self.file)
        else:
            node = InnerNode(degree=self.degree,page=page,file=self.file)
            return self._find_node(key, node)
        
    def insert(self, key,payload):
        ''' TODO: find correct leaf page
                  Handle addition of key to inner page
                  Handle nodes split
        '''
        #print("Btree insert new record")
        #print("Number of pages.."+str(self.num_pages))
        if self.num_pages==1:
            if key in self.root.keys_dict:
                print('Duplicate Key - Insert failed')
                return 'Duplicate Key'
            #insert_node= InnerNode(degree=self.degree,page=1,file=self.file,load_keys=False)
            #insert_node.insert_new_record(key,payload,self)
            if self.root.isFull():
                #print("Node is full")
                #print("Dictionary of Root Leaf before split.."+str(self.root.keys_dict))
                newLeafNode,buble_up_key,lpage,rpage=self.root.insertSplitLeafNode(key, payload, 2,self)
                
                '''Copy first page to page 3 '''
                self.file.seek(0)
                root_page=self.file.read(page_size)
                self.file.seek(page_size*2)
                self.file.write(root_page)
                
                '''Update Root '''
                #self.create_inner_page(page=1)
                self.file.seek(0)
                self.file.write(bytearray(page_size))
                self.file.seek(0)
                root_page_header=(inner_page,0,page_size,0)
                self.file.write(inner_page_header_struct.pack(*root_page_header))
                new_root=InnerNode(degree=self.degree,page=1,file=self.file,load_keys=False)
                new_root.insertInnerKey(buble_up_key,3,2)
                self.root=new_root
                self.num_pages=3
            else:
                #print("Btree before calling insertLeafKey.."+str(key)+str(payload))
                self.root.insertLeafKey(key, payload)
            return
        else:
            #print("Look for insert node and then call insert")
            insert_node=self._find_node(key,self.root)
            #print("After check for duplicate")
            if key in insert_node.keys_dict:
                print("Duplicate key")
                return 'Duplicate_Key'
            #print("Print Root info..",self.root,type(self.root))
            self.root.insert_new_record(key,payload,self)
            return 'Success'
            
        

    def __searchTree(self, key):
        
        pass

    def update(self, anItem):
        ''' If found, update the item with a matching key to be a
          deep copy of anItem and answer anItem.  If not, answer None.
        '''
        pass

def btreemain():
    #print("Btree main")
    return

    lst = [10,8,22,14,12,18,2,50,15,25]
    os.chdir(files_directory)
    file=open("davisbase_columns.tbl",'r+b')
    node = BxTree(degree=5,db_file_name='city.tbl',column_dic_file=file)
    print(node)
    print("Get dictionary")
    print(node._get_record_dictionary())
    
    '''
    payload_fmt=">hib35s"
    payload_struct=Struct(payload_fmt)
    payload=(int(payload_struct.size),int(12),int(12+35),b'TestDBaaaccAAABAAAAAAAAAABBBBBBBBBB')
    status=node.insert(key=12, payload=payload_struct.pack(*payload))
    print(node)
    print(status)
    
    columns_array=[('test12.tbl','row_id',1,'INT','NO','PRI'),
                   ('test12.tbl','test_name',2,'TEXT','YES',''),
                   ('test12.tbl','test_id',3,'INT','NO',''),
                   ('test12.tbl','test_date',4,'DATE','YES','')]
    #node.create_table(table_name='test12.tbl',columns=columns_array)

    for x in lst:
        print("***Inserting",x)
        node.insertLeafKey(x, "Test"+str(x))
        print(repr(node))

    print("***Split Insert 11")
    new_node, buble_up_key = node.insertSplitLeafNode(11, "Test"+str(x))
    print(repr(node))
    print(repr(new_node))
    print("Buble up key..."+str(buble_up_key))
#     lst = [14,50,8,12,18,2,10,22,15]
#     
#     for x in lst:
#         print("***Deleting",x)
#         b.delete(x) 
#         print(repr(b))
    
    '''
    return
    
if __name__ == "__main__":
    btreemain()
