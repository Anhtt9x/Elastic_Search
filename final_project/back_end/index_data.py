from typing import List
from utils import get_es_client
from elasticsearch import Elasticsearch
from config import INDEX_NAME
from tqdm import tqdm
import json


def index_data(documents:List[dict]):
    es = get_es_client()
    _=__create_index(es)
    _=__insert_document(es=es,documents=documents)

def __create_index(es:Elasticsearch) -> dict:
    es.indices.delete(index=INDEX_NAME, ignore_unavailable=True)
    return es.indices.create(index=INDEX_NAME)

def __insert_document(es:Elasticsearch, documents:List[dict]) -> dict:
    operations = []
    for document in tqdm(documents, total=len(documents)):
        operations.append({"index":{"_index":INDEX_NAME}})
        operations.append(document)

    return es.bulk(operations=operations)


if __name__ == "__main__":
    with open("data/apod.json") as f:
        documents = json.load(f)

    index_data(documents)