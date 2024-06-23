import requests
from tqdm.auto import tqdm
from elasticsearch import Elasticsearch

INDEX_NAME = "course-questions"

def get_documents() -> list:
    docs_url = 'https://github.com/DataTalksClub/llm-zoomcamp/blob/main/01-intro/documents.json?raw=1'
    docs_response = requests.get(docs_url)
    documents_raw = docs_response.json()

    documents = []

    for course in documents_raw:
        course_name = course['course']

        for doc in course['documents']:
            doc['course'] = course_name
            documents.append(doc)
    return documents

def index_documents(documents: list) -> Elasticsearch:
    es_client = Elasticsearch('http://localhost:9200')
    index_settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "properties": {
                "text": {"type": "text"},
                "section": {"type": "text"},
                "question": {"type": "text"},
                "course": {"type": "keyword"} 
            }
        }
    }

    # first time uncomment
    # es_client.indices.create(index=INDEX_NAME, body=index_settings)
    for document in tqdm(documents):
        es_client.index(index=index_name, document=document)
    return es_client

def search(es_client: Elasticsearch, query: str) -> list[dict]:
    search_query = {
        "size": 5,
        "query": {
            "bool": {
                "must": {
                    "multi_match": {
                        "query": query,
                        "fields": ["question^4", "text"],
                        "type": "best_fields"
                    }
                },
                "filter": {
                    "term": {
                        "course": "data-engineering-zoomcamp"
                    }
                }
            }
        }
    }
    response = es_client.search(index=INDEX_NAME, body=search_query)
    responses = response['hits']['hits']
    best_match = responses[0]
    print(best_match._score)
    return responses

def main():
    print('Active')
    documents = get_documents()
    es_client = index_documents(documents)
    query = "How do I execute a command in a running docker container?"
    responses = search(es_client, query)
    print(responses)




if __name__ == "__main__":
    main()
