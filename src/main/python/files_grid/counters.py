import threading
threadLock = threading.Lock()
total_req_size = 0
total_resp_size = 0
insert_idx =0

def set_req_size(size):
    global total_req_size
    with threadLock:
        total_req_size = total_req_size + size

def set_resp_size(size):
    global total_resp_size
    with threadLock:
        total_resp_size = total_resp_size + size

def set_insert_idx_size(size):
    global insert_idx
    with threadLock:
        insert_idx = insert_idx + size
