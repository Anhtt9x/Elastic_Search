from config import INDEX_NAME
from utils import get_es_client
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/v1/regular_search")
async def search(search_query: str, skip:int=0, limit:int=10) -> dict:
    es = get_es_client(max_retries=5,sleep_time=5)
    response = es.search(
        index=INDEX_NAME,
        body={
            "query": {
                "multi_match":{
                    "query": search_query,
                    "fields":["title", "explanation"]
                }
            },
            "from":skip,
            "size":limit
        },
        filter_path=["hits.hits._source,hits.hits._score"]
    )

    hits = response['hits']['hits']

    return {'hits':hits}


@app.get("/api/v1/get_docs_per_year_count")
async def get_docs_per_year_count(search_query:str)-> dict:
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
    response = es.search(index=INDEX_NAME,body={
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


    

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)