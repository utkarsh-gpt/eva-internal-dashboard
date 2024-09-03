import numpy as np
from datetime import datetime
import threading
import time
import streamlit as st

def generate_keys(document, all_keys):
    new_keys = set(document.keys()) - all_keys
    if new_keys:
        all_keys.update(new_keys)

def find_and_generate_keys(collection, limit=0, runtime=5):
    all_keys = set()
    stop_event = threading.Event()
    query = collection.find({})

    def process_documents():
        start_time = time.time()
        for document in query:
            if stop_event.is_set() or time.time() - start_time > runtime:
                break
            threading.Thread(target=generate_keys, args=(document, all_keys)).start() 
            
    # print("Starting document processing...")
    find_thread = threading.Thread(target=process_documents)
    find_thread.start()
    
 
    find_thread.join(timeout=runtime)
    
   
    stop_event.set()
    
  
    if find_thread.is_alive():
        # print(f"Stopping after {runtime} seconds...")
        find_thread.join()
    
    all_keys_list = sorted(list(all_keys))
    return all_keys_list

def count_tokens(text):
    return len(text.split(' '))

def check_date_format(date):
    try:
        datetime.fromisoformat(date)
        return True
    except ValueError:
        return False
    
def check_news_exists(collection):
    pipeline = [
        {"$match": {'news': {'$exists': True}}}
    ]
    
    result = collection.aggregate(pipeline)
    
    if result:
        return result[0]
    else:
        return collection

def extract_nested(row):
    for item, key in zip(row, row.keys()):
        if (isinstance(item, list) or isinstance(item, dict)):
            if item:
                st.write(f"{key}:")
                st.dataframe(item)


def pr_token_count(query):
    for item in query:
        promptTokenCount = sum([count_tokens(content['content']) for content in item['prompt']])
        responseTokenCount = count_tokens(item['response'])
        print({"promptTokenCount":promptTokenCount, "responseTokenCount":responseTokenCount})
        return promptTokenCount, responseTokenCount