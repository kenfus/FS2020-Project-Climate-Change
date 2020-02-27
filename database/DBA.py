# -*- coding: utf-8 -*-
"""
Created on Wed Feb 26 16:36:58 2020

@author: Lukas
"""

class DBA(object):
    
    #static variable dba (DBA-object) --> (_ before varname is to declare it as private)
    _dba = None
    _queries = {'key' : 'query'} #queries could be stored alternatively in a key:value file
    
    def __init__(self):
        #setup connection (con) here
        self._con = con
    
    @staticmethod
    def get_DBA():
        #implement singleton pattern
        return dba
    
    def query(self, key):
        #perform query here
        return result