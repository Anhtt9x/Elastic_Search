from typing import List
from utils import get_es_client

def index_data(documents:List[dict]):
    es = get_es_client()