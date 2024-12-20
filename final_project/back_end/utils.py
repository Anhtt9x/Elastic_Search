import time
from pprint import pprint
from elasticsearch import Elasticsearch

def get_es_client(max_retries:int=5, sleep_time:int=5) -> Elasticsearch:
    i=0
    while i<max_retries:
        try:
            es = Elasticsearch("http://localhost:9200/")
            client_info = es.info()
            pprint(client_info.body)
            return es
        
        except Exception as e:
            pprint("Could not connect to Elasticsearch")
            time.sleep(sleep_time)
            i+=1
    raise ConnectionError("Failed to connect to Elasticsearch after {} retries".format(max_retries))
