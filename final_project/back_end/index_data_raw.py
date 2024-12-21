from typing import List
from utils import get_es_client
from elasticsearch import Elasticsearch
from config import INDEX_NAME_DEFAULT,INDEX_NAME_N_GRAM,INDEX_NAME_RAW
from tqdm import tqdm
import json
from pprint import pprint


def index_data(documents:List[dict]):
    pipeline_id = "apod_pipeline"
    es = get_es_client()
    _=__create_pipeline(es=es,pipeline_id=pipeline_id)
    _=__create_index(es=es)
    _=__insert_document(es=es,documents=documents,pipeline_id=pipeline_id)

    pprint(f'Indexed {len(documents)} documents into Elasticsearch index "{INDEX_NAME_RAW}"')

def __create_pipeline(es:Elasticsearch, pipeline_id:str) -> dict:
    pipeline = {
        "description": "A pipeline to process the data",
        "processors": [
            {
                "html_strip":{
                    "field":"explanation"
                }
            },
            {
                "html_strip":{
                    "field":"title"
                }
            }
        ]
    }

    return es.ingest.put_pipeline(id=pipeline_id,body=pipeline)

def __create_index(es:Elasticsearch) -> dict:
    es.indices.delete(index=INDEX_NAME_RAW,ignore_unavailable=True)
    return es.indices.create(index=INDEX_NAME_RAW)


def __insert_document(es:Elasticsearch, documents:List[dict],pipeline_id:str) -> dict:
    operations = []
    for document in tqdm(documents, total=len(documents)):
        operations.append({"index":{"_index":INDEX_NAME_RAW}})
        operations.append(document)

    return es.bulk(operations=operations,pipeline=pipeline_id)


if __name__ == "__main__":
    with open("data/apod_raw.json") as f:
        documents = json.load(f)

    index_data(documents=documents)