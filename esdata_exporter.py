# -*- coding: utf-8 -*-
"""
Created on Tue Jan  8 09:37:35 2019

@author: sunht
"""
import time
import math
import json
from elasticsearch import Elasticsearch
import constants


def init_easltic():  
    #use constants module store server settings.
    esconn = Elasticsearch(constants.API_URL, http_auth=(constants.AUTH_USER, constants.AUTH_PASSWORD))
    return esconn


def get_total_count(esconn, index_name, matches = None):
    body = None
    if matches is not None:
        body = {"query": {
                "match": matches}}
        
    result = esconn.count(index_name, body = body)
    return result["count"]


def scroll_export(esconn, index_name, total_count, page_size = 1000, matches = None):
    body = None 
    page = 1
    exported = 0 
    total_page =  math.ceil(total_count/page_size)
    
    if matches is not None:
        body = {"query": {
                "match": matches}}
        
    params = {"size": page_size,
              "from": (page-1)*page_size, 
              "scroll": "1m"}
    
    print("exporting page:", page, "/", total_page)
    result = esconn.search(index_name, body= body, params = params)
    hits = result["hits"]["hits"]
    exported = exported + len(hits)
    
    #store exported result to data folder
    dump_to_file(hits, "data/{}_{}.json".format(index_name, page))
    
    
    while exported < total_count:    
        page = page + 1
        print("exporting page:", page, "/", total_page)
        result = esconn.scroll(result["_scroll_id"], params = {"scroll": "1m"})
        hits = result["hits"]["hits"]
        exported = exported + len(hits)
        dump_to_file(hits, "data/{}_{}.json".format(index_name, page))
        if len(hits) < page_size:            
            # no more data
            break
            
        time.sleep(0.1)
    
    return exported

def dump_to_file(data, file_name):
    f = open(file_name, mode="w")
    json.dump(data, f)
    
    f.close()

if __name__ == "__main__":
    index_name = 'logstash-2018.11.29'
    matches = {
            "kubernetes.namespace_name": "sxz"
            }
    
    esconn = init_easltic()
    total_count = get_total_count(esconn, index_name, matches)
    print("total data to be exported:", total_count)
    
    exported = scroll_export(esconn, index_name, total_count, matches = matches)
            
    print("export process complete, data items exported:", exported)
    
    
            
    
        
    
    
    
    