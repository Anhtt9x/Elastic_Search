from config import INDEX_NAME_DEFAULT,INDEX_NAME_N_GRAM,INDEX_NAME_EMBEDDING,INDEX_NAME_RAW
from utils import get_es_client
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from fastapi.responses import HTMLResponse
import torch
from sentence_transformers import SentenceTransformer

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model = SentenceTransformer("all-MiniLM-L6-v2").to(device)

@app.get("/api/v1/regular_search")
async def regular_search(search_query: str, skip:int=0, limit:int=10, year:str | None =None,) -> dict:
    es = get_es_client(max_retries=1,sleep_time=0)
    
    query = {
            "bool":{
                "must":[
                    {
                        "multi_match":{
                            "query":search_query,
                            "fields":["title", "explanation"]
                        }
                    }
                ]
            }
        }
    
    if year:
        query["bool"]["filter"] = [
            {
                "range": {
                    "date":{
                        "gte":f"{year}-01-01",
                        "lte":f"{year}-12-31",
                        "format":"yyyy-MM-dd"
                    }
                }
            }
        ]

    response = es.search(
        index=INDEX_NAME_RAW,
        body={
            "query":query,
            "from":skip,
            "size":limit
        },
        filter_path=["hits.hits._source",
                     "hits.hits._score",
                     "hits.total"]
    )
    total_hits = response["hits"]["total"]["value"]
    max_pages = (total_hits + limit -1)// limit
    hits = response['hits']['hits']

    return {'hits':hits,
            "max_pages":max_pages}


@app.get("/api/v1/semantic_search")
async def semantic_search(search_query: str, skip:int=0, limit:int=10, year:str | None =None,) -> dict:
    es = get_es_client(max_retries=1,sleep_time=0)
    embedded_query = model.encode(search_query)
    query = {
        "bool":{
            "must":[
                {
                    "knn":{
                        "field":"embedding",
                        "query_vector":embedded_query,
                        "k":1e4,
                    }
                }
            ]
        }
    }

    if year:
        query["bool"]["filter"] = [
            {
                "range": {
                    "date":{
                        "gte":f"{year}-01-01",
                        "lte":f"{year}-12-31",
                        "format":"yyyy-MM-dd"
                    }
                }
            }
        ]
    
    response = es.search(
        index=INDEX_NAME_EMBEDDING,
        body={
            "query":query,
            "from":skip,
            "size":limit
        },
        filter_path=["hits.hits._source",
                     "hits.hits._score",
                     "hits.total"]
    )
    total_hits = response["hits"]["total"]["value"]
    max_pages = (total_hits + limit -1)// limit
    hits = response['hits']['hits']

    return {'hits':hits,
            "max_pages":max_pages}



@app.get("/api/v1/get_docs_per_year_count")
async def get_docs_per_year_count(search_query:str)-> dict:
    try:
        es = get_es_client(max_retries=1,sleep_time=0)
        query = {
            "bool":{
                "must":[
                    {
                        "multi_match":{
                            "query":search_query,
                            "fields":["title", "explanation"]
                        }
                    }
                ]
            }
        }
        response = es.search(index=INDEX_NAME_RAW,
            body={
                "query":query,
                "aggs":{
                    "docs_per_year": {
                        "date_histogram": {
                            "field": "date",
                            "calendar_interval":"year",
                            "format":"yyyy"
                        }
                    }
                }
        },
        filter_path=["aggregations.docs_per_year"]
        )

        buckets = response['aggregations']['docs_per_year']['buckets']

        return {'docs_per_year':{bucket["key_as_string"]:bucket["doc_count"] for bucket in buckets}}
    
    except Exception as e:
        return HTMLResponse(content=str(e), status_code=500)

    

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)