from typing import List
from utils import get_es_client
from elasticsearch import Elasticsearch
from config import INDEX_NAME_DEFAULT,INDEX_NAME_N_GRAM
from tqdm import tqdm
import json
from pprint import pprint


def index_data(documents:List[dict], use_n_gram_tokenizer:bool=False):
    es = get_es_client()
    _=__create_index(es=es,use_n_gram_tokenizer=use_n_gram_tokenizer)
    _=__insert_document(es=es,documents=documents,use_n_gram_tokenizer=use_n_gram_tokenizer)

    index_name = INDEX_NAME_DEFAULT if not use_n_gram_tokenizer else INDEX_NAME_N_GRAM
    pprint(f'Indexed {len(documents)} documents into Elasticsearch index "{index_name}"')



def __create_index(es:Elasticsearch,use_n_gram_tokenizer:bool) -> dict:
    tokenizer = "n_gram_tokenizer" if use_n_gram_tokenizer else "standard"
    index_name = INDEX_NAME_DEFAULT if not use_n_gram_tokenizer else INDEX_NAME_N_GRAM

    es.indices.delete(index=index_name,ignore_unavailable=True)
    return es.indices.create(index=index_name,
                       body={
            "settings": {
                "analysis": {  # Đặt tokenizer và analyzer trong `analysis`
                    "tokenizer": {
                        "n_gram_tokenizer": {
                            "type": "edge_ngram",
                            "min_gram": 1,
                            "max_gram": 30,
                            "token_chars": ["letter", "digit"]
                        }
                    },
                    "analyzer": {
                        "default": {
                            "type": "custom",
                            "tokenizer": tokenizer
                        }
                    }
                }
            }
        })


def __insert_document(es:Elasticsearch, documents:List[dict],use_n_gram_tokenizer:bool) -> dict:
    operations = []
    index_name = INDEX_NAME_DEFAULT if not use_n_gram_tokenizer else INDEX_NAME_N_GRAM
    for document in tqdm(documents, total=len(documents)):
        operations.append({"index":{"_index":index_name}})
        operations.append(document)

    return es.bulk(operations=operations)


if __name__ == "__main__":
    with open("data/apod.json") as f:
        documents = json.load(f)

    index_data(documents=documents,use_n_gram_tokenizer=True)