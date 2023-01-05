from elasticsearch import Elasticsearch, helpers
import json
import warnings
from elasticsearch.helpers import bulk

# import data in json format
file_name = 'IR_data_news_12k.json'

with open(file_name) as f:
    data = json.load(f)

# Filter warnings
warnings.filterwarnings('ignore')

# data keys
data['0'].keys()

es = Elasticsearch(['http://localhost:9200'], http_auth=('elastic', '7s-sKB6EPzut8lMdf6bF'))

sm_index_name = 'tfidf_index'

# Delete index if one does exist
if es.indices.exists(index=sm_index_name):
    es.indices.delete(index=sm_index_name)

# Create index
es.indices.create(index=sm_index_name)


def bulk_sync():
    actions = [
        {
            '_index': sm_index_name,
            '_id': doc_id,
            '_source': doc
        } for doc_id, doc in data.items()
    ]
    bulk(es, actions)

# run the function to add documents
bulk_sync()

# Check index
es.count(index = sm_index_name)

# TODO : uncomment the code bellow, write the tf-idf code in here
#source_code =

# closing the index
es.indices.close(index=sm_index_name)

# applying the settings
es.indices.put_settings(index=sm_index_name,
                            settings={
                                "similarity": {
                                      "default": {
                                        "type": "scripted",
                                        "script": {
                                          # TODO : uncomment the code bellow and pass the suitable parameter
                                           "source": "double tf = Math.sqrt(doc.freq); double idf = Math.log((field.docCount+1.0)/(term.docFreq+1.0)) + 1.0; double norm = 1/Math.sqrt(doc.length); return query.boost * tf * idf * norm;"
                                        }
                                      }
                                }
                            }
                       )

# reopening the index
es.indices.open(index=sm_index_name)


# A function that creates appropriate body for our match content type query
def get_query(text):
    body = {
        "query": {
            "match": {
                "content": text

            }
        }
    }

    return body

queries = ["ایالات متحده آمرکا","استانبول"]

all_res_tfidf = []


for q in queries:
    res_tfidf = es.search(index=sm_index_name, body=get_query(q), explain=True)
    all_res_tfidf.append(dict(res_tfidf))

for res, q in zip(all_res_tfidf, queries):
    print(q)
    for doc in res['hits']['hits']:
        print(doc['_source']['url'])
    print("----------------------------")
