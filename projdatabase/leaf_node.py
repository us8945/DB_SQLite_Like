'''
Created on Jul 19, 2016

@author: uri
'''
import datetime
import os
from copy import deepcopy
import sys
import math
from collections import OrderedDict
from struct import Struct
import struct

page_size = 512
db_files='data'
tables_dictionary='davisbase_tables.tbl'
columns_dictionary='davisbase_columns.tbl'
root_loc = os.path.dirname(os.path.abspath(__file__))
files_directory= os.path.join(root_loc,db_files)

page_header_fmt=">bbH"
page_header_struct=Struct(page_header_fmt)
record_header_fmt='>Hi'
record_header_struct=Struct(record_header_fmt)

class LeafNode:   
    def __init__(self, page=1, degree = 1, keys_dict = None,last_key=None,file=None):
        ''' Creating an empty node with the indicated degree'''
        #print("Create new instance of Leaf Node....")
        self.numberOfKeys = 0
        self.degree = degree
        self.page = page
        self.file=file
        self.header = self._read_page_header()
        self.numberOfKeys = self.header[1]
        self.offset = self.header[2]
        if keys_dict is None:
            if self.numberOfKeys==0:
                self.keys_dict={}
                self.last_key=None
            else:
                self.keys_dict,self.last_key=self._load_keys()
        else:
            self.keys_dict=keys_dict
            self.numberOfKeys=len(keys_dict.keys())
        
        #print("Offset in init.."+str(self.offset))

    def __repr__(self):
        return "LeafNodeClass("+str(self.degree)+","+str(self.numberOfKeys)+","+repr(self.keys_dict)+","+str(self.page)+")"

    
    def _read_page_header(self):
        self.file.seek((self.page-1)*page_size)
        header=page_header_struct.unpack(self.file.read(page_header_struct.size))
        return header
    
    def _load_keys(self):
        offsets=self.file.read(self.numberOfKeys*2)
        offsets_fmt='>'
        for i in range(0,self.numberOfKeys):
            offsets_fmt=''.join((offsets_fmt,'H'))
        #print(self.numberOfKeys)
        #print(offsets_fmt)
        offsets_struct=Struct(offsets_fmt)
        offsets = offsets_struct.unpack(offsets)
        #print(offsets)
        #print(str(len(offsets)))
        keys={}
        for i in range(0,self.numberOfKeys):
            #print(i)
            #print(offsets[i])
            #print((self.page-1)*page_size+offsets[i])
            self.file.seek((self.page-1)*page_size+offsets[i])
            record_header=record_header_struct.unpack(self.file.read(record_header_struct.size))
            key=record_header[1]
            key_offsets=[offsets[i],offsets[i]+record_header[0]]
            record_size=record_header[0]
            self.file.seek((self.page-1)*page_size+offsets[i])
            payload=self.file.read(record_size)
            #print("Load Keys..record header..",record_header[0],record_header[1])
            #print("Load Keays..payload..",payload)
            keys[key]=(key_offsets,payload)
        
        self.file.seek((self.page-1)*page_size+self.header[2])
        last_record_header=record_header_struct.unpack(self.file.read(record_header_struct.size))
        #print("Load keys, keys and lask key..",keys,last_record_header[1])
        return keys,last_record_header[1]
        
    def setPage(self,new_page):
        self.page=new_page
        
    def insertSplitLeafNode(self, key, payload,new_page,tree):
        '''
        Split Leaf node: leave left part in place, move right part to new node
        Return middle-key
        Return new Leaf Node
        '''
        if self.degree > self.numberOfKeys:
            #print("There is more space left. No need to Split")
            return 

        offsets= [0 - len(payload), 0]
        self.numberOfKeys+=1    
        self.keys_dict[key] = (offsets, payload)
        
        #print("Keys dictionary before split.."+str(self.keys_dict))
        
        #print ("Before Splitting for two Nodes")
        '''Calculate number of items in to be left in original node'''
        if (self.degree +1)%2 ==0:
            split_index=int(((self.degree+1)/2))
        else:
            split_index=int(math.ceil(self.degree/2))
       
        '''Re-destribute keys and payload, plus recalculate offsets after splitting to two nodes'''
        #print("Split index.."+str(split_index)) 
        #print(sorted(self.keys_dict))    
        self.numberOfKeys = 0
        index=0
        orig_node_dict = {}
        new_node_dict={}
        offsets = []
        new_dic_numberOfKeys=0
        for key in sorted(self.keys_dict):
            if index < split_index:
                if self.numberOfKeys==0:
                    offsets=[page_size - (self.keys_dict[key][0][1] - self.keys_dict[key][0][0]),page_size]
                    orig_node_dict[key] = (offsets,self.keys_dict[key][1])
                    self.numberOfKeys+=1
                else:
                    offsets=[offsets[0] - (self.keys_dict[key][0][1] - self.keys_dict[key][0][0]),offsets[0]]
                    orig_node_dict[key] = (offsets,self.keys_dict[key][1])
                    self.numberOfKeys+=1
            else:
                if new_dic_numberOfKeys==0:
                    offsets=[page_size - (self.keys_dict[key][0][1] - self.keys_dict[key][0][0]),page_size]
                    new_node_dict[key] = (offsets,self.keys_dict[key][1])
                    new_dic_numberOfKeys+=1
                else:
                    offsets=[offsets[0] - (self.keys_dict[key][0][1] - self.keys_dict[key][0][0]),offsets[0]]
                    new_node_dict[key] = (offsets,self.keys_dict[key][1])
                    new_dic_numberOfKeys+=1
            index+=1

        self.keys_dict = orig_node_dict
        buble_up_key=sorted(new_node_dict)[0]
        '''
            TODO - change new page calculations
            '''
        tree.create_leaf_page(new_page)
        #print("New Split Leaf Node dictionary:")
        #print(new_node_dict)
        newLeafNode=LeafNode(page=new_page,degree=self.degree, keys_dict=new_node_dict,file=self.file)
        
        newLeafNode._reWriteNode()
        '''
        TODO: save old Node to file
        '''
        
        self._reWriteNode()
        lpage=self.page
        rpage=newLeafNode.page
        return newLeafNode,buble_up_key,lpage,rpage
    
    def _writeNewNode(self, new_node_dict):
        pass
    
    def _reWriteNode(self):
        '''Initialize page
        '''
        #print("Inside reWrite Leaf Node")
        self.file.seek((self.page-1)*page_size)
        self.file.write(bytearray(page_size))
        
        '''Write offsets'''
        self.numberOfKeys=len(self.keys_dict.keys())
        offsets_fmt='>'
        for i in range(0,self.numberOfKeys):
            offsets_fmt=''.join((offsets_fmt,'H'))
        
        offsets=[]
        start_off_set=page_size
        for key in sorted(self.keys_dict):
            start_off_set=start_off_set-len(self.keys_dict[key][1])
            offsets.append(start_off_set)
            self.offset=start_off_set
        #print("Offsets, and starting offset..",offsets,self.offset)
            
        self.file.seek((self.page-1)*page_size+4)
        #print("Offsets formating.."+offsets_fmt) 
        #print("Offsets array.."+str(offsets))   
        #print("Keys dictionary.."+str(self.keys_dict))
        offsets_struct=Struct(offsets_fmt)
        self.file.write(offsets_struct.pack(*offsets))
        
        '''Write payload'''
        off_set_before_write=[]
        self.file.seek((self.page-1)*page_size+self.offset)
        for key in sorted(self.keys_dict,reverse=True):
            off_set_before_write.append(self.file.tell())
            self.file.write(self.keys_dict[key][1])
            #print("Write payload..",self.keys_dict[key],self.keys_dict[key][1])
        #print("Offsets before write are..",off_set_before_write)
        
        '''Write page header'''
        self.header = (self.header[0], self.numberOfKeys, self.offset)
        #print("Leaf..Header before re-write..",self.header,self.page)
        self.file.seek((self.page-1)*page_size)
        self.file.write(page_header_struct.pack(*self.header))
        
        return
    
    def _writeLastRecord(self):
        '''Write page header'''
        self.header = (self.header[0], self.numberOfKeys, self.offset)
        #print("Header before write last record..",self.header)
        self.file.seek((self.page-1)*page_size)
        self.file.write(page_header_struct.pack(*self.header))
        
        '''Write offsets'''
        self.numberOfKeys=len(self.keys_dict.keys())
        offsets_fmt='>'
        for i in range(0,self.numberOfKeys):
            offsets_fmt=''.join((offsets_fmt,'H'))
        
        offsets=[]
        for key in sorted(self.keys_dict):
            offsets.append(self.keys_dict[key][0][0])
        
        #print("Offsets in Last record write"+str(offsets))
        self.file.seek((self.page-1)*page_size+4)    
        offsets_struct=Struct(offsets_fmt)
        self.file.write(offsets_struct.pack(*offsets))
        
        '''Write payload'''
        self.file.seek((self.page-1)*page_size+self.offset)
        self.file.write(self.keys_dict[self.last_key][1])
        
        return
        
    def insertLeafKey(self, key, payload):  
        '''
        Assume that no split is needed
        '''
        #print("Number of keys"+str(self.numberOfKeys))
        #print("Key and len payload.."+str(key)+","+str(len(payload)))
        if self.numberOfKeys == 0:
            offsets = [page_size - len(payload),page_size]
            self.offset = page_size - len(payload)
        else:
            #last_key = next(reversed(self.keys_dict))
            last_offset = self.keys_dict[self.last_key][0][0]
            offsets= [last_offset - len(payload), last_offset]
            self.offset = last_offset - len(payload)
        
        self.numberOfKeys+=1    
        self.keys_dict[key] = (offsets, payload)
        self.last_key = key
        self._writeLastRecord()
        return

    def isFull(self):
        ''' Answer True if the receiver is full.  If not, return
          False.
        '''
        if self.numberOfKeys >= self.degree:
            #print("True")
            return True
        else:
            #print("False")
            return False

    def removeKey(self, key):
        '''
        Remove key from the list of keys. First phase, no tree re-balancing 
        TODO: Re-balance the Tree 

        '''
        pass

    def searchNode(self, key):
        '''
        Return page and off-set of the data-record (payload) location on the page
        '''
        pass       

    def setNumberOfKeys(self, numKeys ):
        self.numberOfKeys = numKeys




def btreemain():
    #print("My/Our name(s) is/are ")
    os.chdir(files_directory)
    file = open("davisbase_columns.tbl",'r+b')
    node=LeafNode(degree=5,page=5,file=file)
    print(node)
    file.close()
    '''
    payload_fmt=">hib11s"
    payload_struct=Struct(payload_fmt)
    payload=(int(payload_struct.size),int(18),int(11),b'TestDBaaacc')
    print(str(payload_struct.pack(*payload)))
    node.insertLeafKey(18, payload_struct.pack(*payload))
    file.close()

    payload_fmt=">hib12s"
    payload_struct=Struct(payload_fmt)
    payload=(int(payload_struct.size),int(12),int(12),b'TestDBaaaccA')
    print(str(payload_struct.pack(*payload)))
    node.insertLeafKey(12, payload_struct.pack(*payload))
    file.close()
    '''
    '''
    payload_fmt=">hib13s"
    payload_struct=Struct(payload_fmt)
    payload=(int(payload_struct.size),int(100),int(13),b'TestDBaaaccAA')
    print(str(payload_struct.pack(*payload)))
    node.insertLeafKey(100, payload_struct.pack(*payload))
    file.close()
    '''
    '''
    payload_fmt=">hib15s"
    payload_struct=Struct(payload_fmt)
    payload=(int(payload_struct.size),int(4),int(15),b'TestDBaaaccAAAB')
    print(str(payload_struct.pack(*payload)))
    node.insertLeafKey(4, payload_struct.pack(*payload))
    '''
    
    
    '''
    lst = [10,8,22,14,12,18,2,50,15,25]
    
    node = LeafNode(degree=10)
    
    for x in lst:
        print("***Inserting",x)
        node.insertLeafKey(x, "Test"+str(x))
        print(repr(node))

    print("***Split Insert 11 and page 2")
    new_node, buble_up_key = node.insertSplitLeafNode(11, "Test"+str(x),2)
    print(repr(node))
    print(repr(new_node))
    print("Buble up key..."+str(buble_up_key))
    
    '''
    
    return  
    
if __name__ == "__main__":
    btreemain()