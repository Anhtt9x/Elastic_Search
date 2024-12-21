from typing import List
from utils import get_es_client
from elasticsearch import Elasticsearch
from config import INDEX_NAME_EMBEDDING
from tqdm import tqdm
import json
from pprint import pprint
from sentence_transformers import SentenceTransformer
import torch


def index_data(documents:List[dict], model:SentenceTransformer):
    es = get_es_client(max_retries=5,sleep_time=5)
    _=__create_index(es=es)
    _=__insert_document(es=es,documents=documents,model=model)

    pprint(f'Indexed {len(documents)} documents into Elasticsearch index "{INDEX_NAME_EMBEDDING}"')



def __create_index(es:Elasticsearch,) -> dict:
    es.indices.delete(index=INDEX_NAME_EMBEDDING,ignore_unavailable=True)
    es.indices.create(index=INDEX_NAME_EMBEDDING,
                      mappings={
                        "properties":{
                            "embedding":{
                                "type":"dense_vector",
                                "dims":384
                            }
                        }
                    })
                


def __insert_document(es:Elasticsearch,documents:List[dict],model:SentenceTransformer) -> dict:
    operations = []

    for document in tqdm(documents, total=len(documents),desc="Indexing documents"):
        embedding = model.encode(document['explanation'])
        operations.append({"index":{"_index":INDEX_NAME_EMBEDDING}})
        operations.append({**document,
                           "embedding":embedding})

    return es.bulk(operations=operations)


if __name__ == "__main__":
    with open("data/apod.json") as f:
        documents = json.load(f)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = SentenceTransformer("all-MiniLM-L6-v2").to(device)

    index_data(documents=documents,model=model)