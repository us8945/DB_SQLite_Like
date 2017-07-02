'''
Created on Jul 19, 2016

@author: uri
'''
import datetime
import os
#from copy import deepcopy
import sys
import math
#from collections import OrderedDict
from struct import Struct
import struct
from projdatabase.leaf_node import LeafNode
from django.db.migrations.graph import Node

page_size = 512
#inner_cell_size = 8
db_files='data'
tables_dictionary='davisbase_tables.tbl'
columns_dictionary='davisbase_columns.tbl'
root_loc = os.path.dirname(os.path.abspath(__file__))
files_directory= os.path.join(root_loc,db_files)

inner_page_header_fmt=">bbHI"
inner_page_header_struct=Struct(inner_page_header_fmt)
record_fmt='>ii'
record_struct=Struct(record_fmt)
inner_cell_size=record_struct.size
key_offset_fmt=">H"
key_offset_struct=Struct(key_offset_fmt)

leaf_page_header_fmt=">bbH"
leaf_page_header_struct=Struct(leaf_page_header_fmt)

leaf_page=0x0d
inner_page=0x05

class InnerNode:   
    def __init__(self, page=1, degree = 1, keys_dict = None,last_key=None,file=None,load_keys=None):
        ''' Creating an empty node with the indicated degree'''
        #print("Create new instance of Inner Node....")
        self.numberOfKeys = 0
        self.degree = degree
        self.page = page
        self.file = file
        #self.keys_dict=OrderedDict()
        
        self.header = self._read_page_header()
        self.numberOfKeys = self.header[1]
        self.offset = self.header[2]
        self.right_page_pointer = self.header[3]
        if not(keys_dict is None or keys_dict=={}):
            #print('keys_dict',keys_dict)
            self.keys_dict = keys_dict
            self.last_key = max(keys_dict.keys())
            self.max_key = max(keys_dict.keys())
            self.numberOfKeys = len(keys_dict.keys())
        else: 
            if load_keys is None: 
                self.keys_dict,self.last_key,self.max_key=self._load_keys()
            else:
                self.keys_dict={}
            
        #print(str(self.numberOfKeys)+","+str(self.offset)+","+str(self.right_page_pointer))

    def __repr__(self):
        return "InnerNodeClass("+str(self.degree)+","+str(self.numberOfKeys)+ \
            ","+repr(self.keys_dict)+","+str(self.page)+","+str(self.right_page_pointer)+"\n"

    def _read_page_header(self):
        self.file.seek((self.page-1)*page_size)
        header=inner_page_header_struct.unpack(self.file.read(inner_page_header_struct.size))
        return header
    
    def _load_keys(self):
        offsets=self.file.read(self.numberOfKeys*2)
        offsets_fmt='>'
        for i in range(0,self.numberOfKeys):
            offsets_fmt=''.join((offsets_fmt,'H'))
        offsets_struct=Struct(offsets_fmt)
        offsets = offsets_struct.unpack(offsets)
        keys={}
        for i in range(0,self.numberOfKeys):
            self.file.seek((self.page-1)*page_size+offsets[i])
            record=record_struct.unpack(self.file.read(record_struct.size))
            key=record[1]
            left_page=record[0]
            key_offsets=[offsets[i],offsets[i]+record_struct.size]
            keys[key]=(key_offsets,left_page)
        
        self.file.seek((self.page-1)*page_size+self.offset)
        last_record=record_struct.unpack(self.file.read(record_struct.size))
        return keys,last_record[1],max(keys.keys(),key=int) #Return dictionary, last key and max key
    
    def insert_new_record(self,key,payload,tree):
        #print("Insert new record- inner node")
        page = self.searchNode(key)
        #print("Page to insert new record.."+str(page))
        self.file.seek((page-1)*page_size)
        page_header=leaf_page_header_struct.unpack(self.file.read(leaf_page_header_struct.size))
        if page_header[0]==leaf_page:
            leaf_node=LeafNode(degree=self.degree,page=page,file=self.file)
            if leaf_node.isFull():
                new_page=tree.num_pages+1
                tree.num_pages+=1
                newLeafNode,buble_up_key,lpage,rpage=leaf_node.insertSplitLeafNode(key, payload, new_page,tree)
                if self.isFull():
                    new_page=tree.num_pages+1
                    tree.num_pages+=1
                    newInnerNode,buble_up_key,lpage,rpage=self.insertSplitInnerNode(buble_up_key, lpage, rpage, new_page,tree)
                    return newInnerNode,buble_up_key,lpage,rpage
                else:
                    self.insertInnerKey(buble_up_key, lpage, rpage)
                    return
            else:
                leaf_node.insertLeafKey(key, payload)
                return
        else:
            inner_node = InnerNode(degree=self.degree,page=page,file=self.file)
            newInnerNode,buble_up_key,lpage,rpage = inner_node.insert_new_record(key,payload,tree)
            if buble_up_key is not None:
                if self.isFull():
                    new_page=tree.num_pages+1
                    tree.num_pages+=1
                    newInnerNode,buble_up_key,lpage,rpage=self.insertSplitInnerNode(buble_up_key, lpage, rpage, new_page)
                    return newInnerNode,buble_up_key,lpage,rpage
                else:
                    self.insertInnerKey(buble_up_key, lpage, rpage)
                    return
                
            return
        
        return
            
    
    
    def insertSplitInnerNode(self, key, lpage,rpage,new_page,tree):
        '''
        Inner Leaf node: leave left part in place, move right part to new node
        Return middle-key
        Return new Inner Node
        '''
        '''The key is highest key so far, insert and update right page pointer'''
        if (key>self.max_key):
            last_offset = self.keys_dict[self.last_key][0][0]
            offsets= [last_offset-1 - inner_cell_size, last_offset-1]
            self.keys_dict[key] = (offsets, lpage)
            self.right_page_pointer = rpage
            '''Insert key in the middle - swap pointers with min key greater than new key'''
        else:
            key_after = min(k for k in self.keys_dict if k > key)
            last_offset = self.keys_dict[self.last_key][0][0]
            self.keys_dict[key_after]=(self.keys_dict[key_after][0],rpage)
            offsets= [last_offset-1 - inner_cell_size, last_offset-1]
            self.keys_dict[key] = (offsets, lpage)
        
        self.numberOfKeys+=1    
        self.last_key = key
        
        #print ("Before Splitting for two Nodes")
        '''Calculate number of items in to be left in original node'''
        if (self.degree +1)%2 ==0:
            split_index=int(((self.degree+1)/2))
        else:
            split_index=int(math.ceil(self.degree/2))
       
        '''Re-destribute keys and pointers, plus recalculate offsets after splitting to two nodes'''
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
                    offsets=[offsets[0] - (self.keys_dict[key][0][1] - self.keys_dict[key][0][0])-1,offsets[0]-1]
                    orig_node_dict[key] = (offsets,self.keys_dict[key][1])
                    self.numberOfKeys+=1
            elif index==split_index:
                buble_up_key=key
                self.right_page_pointer=self.keys_dict[key][1]
            else:
                if new_dic_numberOfKeys==0:
                    offsets=[page_size - (self.keys_dict[key][0][1] - self.keys_dict[key][0][0]),page_size]
                    new_node_dict[key] = (offsets,self.keys_dict[key][1])
                    new_dic_numberOfKeys+=1
                else:
                    offsets=[offsets[0] - (self.keys_dict[key][0][1] - self.keys_dict[key][0][0])-1,offsets[0]-1]
                    new_node_dict[key] = (offsets,self.keys_dict[key][1])
                    new_dic_numberOfKeys+=1
            index+=1

        self.keys_dict = orig_node_dict
        '''
            TODO - change new page calculations
            '''
        tree.create_inner_page(new_page)
        newInnerNode=InnerNode(page=new_page,degree=self.degree, keys_dict=new_node_dict,file=self.file)
        
        newInnerNode._reWriteNode()

        self._reWriteNode()
        lpage=self.page
        rpage=newInnerNode.page
        return newInnerNode,buble_up_key,lpage,rpage
    
    def _writeNode(self):
        ''' Can use only if key is added to the beginning or to the end
        '''
        self.offset-=record_struct.size
        '''Write/update page header'''
        self.header = (self.header[0], self.numberOfKeys, self.offset,self.right_page_pointer)
        #print("Header before re-write..",self.header)
        self.file.seek((self.page-1)*page_size)
        self.file.write(inner_page_header_struct.pack(*self.header))
        
        '''Write offsets'''
        offsets_fmt='>'
        offsets=[]
        for key in sorted(self.keys_dict.keys()):
            offsets.append(self.keys_dict[key][0][0])
            offsets_fmt=''.join((offsets_fmt,'H'))
            
        self.file.seek((self.page-1)*page_size+inner_page_header_struct.size)    
        offsets_struct=Struct(offsets_fmt)
        self.file.write(offsets_struct.pack(*offsets))
        
        
        '''Write last added record
        '''
        self.file.seek((self.page-1)*page_size+self.offset)
        record=(int(self.keys_dict[self.last_key][1]),int(self.last_key))
        self.file.write(record_struct.pack(*record))
        
        return
    
    def _reWriteNode(self):
        '''Initialize page
        '''
        self.file.seek((self.page-1)*page_size)
        self.file.write(bytearray(page_size))
        
        '''Write offsets'''
        self.numberOfKeys=len(self.keys_dict.keys())
        offsets_fmt='>'
        for i in range(0,self.numberOfKeys):
            offsets_fmt=''.join((offsets_fmt,'H'))
        
        offsets=[]
        self.offset=page_size - record_struct.size*len(self.keys_dict.keys())
        start_off_set=self.offset
        for key in sorted(self.keys_dict.keys()):
            #print("Key and offset before write.."+str(key)+","+str(start_off_set))
            offsets.append(start_off_set)
            start_off_set+=record_struct.size
            
        self.file.seek((self.page-1)*page_size+inner_page_header_struct.size)    
        offsets_struct=Struct(offsets_fmt)
        self.file.write(offsets_struct.pack(*offsets))

        
        self.file.seek((self.page-1)*page_size+self.offset)
        for key in sorted(self.keys_dict):
            record=(int(self.keys_dict[key][1]),int(key))
            self.file.write(record_struct.pack(*record))
        
        '''Write page header'''
        self.header = (self.header[0], self.numberOfKeys, self.offset,self.right_page_pointer)
        #print("Header before re-write..",self.header)
        self.file.seek((self.page-1)*page_size)
        self.file.write(inner_page_header_struct.pack(*self.header))
        
        return
    
        
    def insertInnerKey(self, key, lpage,rpage):  
        '''
        Assume that no split is needed
        '''
        #print("Number of keys"+str(self.numberOfKeys))
        #print("Key, left page and right page.."+str(key)+","+str(lpage)+","+str(rpage))
        ''' The Dictionary is empty - insert first key'''
        if self.numberOfKeys == 0:
            offsets = [page_size - inner_cell_size-1,page_size]
            self.keys_dict[key]=(offsets,lpage)
            self.right_page_pointer=rpage
            self.max_key=key
            self.numberOfKeys+=1    
            self.last_key = key
            self._reWriteNode()
            '''The key is highest key so far, insert and update right page pointer'''
        elif (key>self.max_key):
            last_offset = self.keys_dict[self.last_key][0][0]
            offsets= [last_offset- inner_cell_size, last_offset-1]
            self.keys_dict[key] = (offsets, lpage)
            self.right_page_pointer = rpage
            self.max_key=key
            self.numberOfKeys+=1    
            self.last_key = key
            self._writeNode()
            '''Insert key in the middle - swap pointers with min key greater than new key'''
        else:
            key_after = min(k for k in self.keys_dict if k > key)
            #print("Key after.."+str(key_after))
            self.keys_dict[key_after]=(self.keys_dict[key_after][0],rpage)
            offsets= [self.offset - inner_cell_size, self.offset-1]
            self.keys_dict[key] = (offsets, lpage)
            self.numberOfKeys+=1    
            self.last_key = key
            self._reWriteNode()
        
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
        Returns page number to look next 
        '''
        for k in sorted(self.keys_dict):
            if key<k:
                return self.keys_dict[k][1]
            
        return self.right_page_pointer   

    def setNumberOfKeys(self, numKeys ):
        self.numberOfKeys = numKeys




def btreemain():
    os.chdir(files_directory)
    file = open("city_new.tbl",'r+b')
    node=InnerNode(degree=10,page=1,file=file)
    print(node)
    del node
    
    lst = [(10,2,3),(8,2,4),(22,3,5),(14,14,20),(12,12,17),(18,18,38),(2,15,33),(50,50,100),(33,33,44)]
    
    node=InnerNode(degree=10,page=1,file=file)
    #node.insertInnerKey(10,3,5)
    #node.insertInnerKey(5,3,7)
    #node.insertInnerKey(1,2,10)
    node.insertInnerKey(15,5,15)
    del node
    '''
    for x in lst:
        print("***Inserting",x)
        node=InnerNode(degree=10,page=1,file=file)
        node.insertInnerKey(x[0],x[1],x[2])
        print(repr(node))
        del node
    '''
    '''
    print("***Split Insert 101 and page 101")
    new_node, buble_up_key = node.insertSplitInnerNode(101, 70,150,200)
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