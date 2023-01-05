#spelling correction
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import json
from tqdm import tqdm
import re
from copy import deepcopy

dataset_path = 'IR_data_news_12k.json'
with open(dataset_path) as f:
    data = json.load(f)


data_tmp = []
for i in tqdm(range(len(data))):
    doc = data[f'{i}']
    content = {
        "content_title":doc['content']
    }
    title = {
        "content_title":doc['title']
    }
    data_tmp.append(content)
    data_tmp.append(title)

def load_data_to_elastic(es,data,index_name):
    data_bulk = [
        {
            "_index" : index_name,
            "_id" : i + 1,
            "_source": data[i]
        }
        for i in range(len(data))
    ]
    resp = helpers.bulk(
        es,
        data_bulk,
        index = index_name
    )
    print(resp)

sc_mapping = {
    "settings": {
    "index": {
"char_filter": [
    {
      "type": "mapping",
      "mappings": [
        "   => 00100000",

      ]
    }
    ],
  "tokenizer": "whitespace",
  "filter": [
    {
      "type": "shingle",
      "min_shingle_size": 2,
      "max_shingle_size": 3
    }
  ]


      #TODO: define your analyzers here
    }
  },
    "mappings": {
        "properties": {
            "content_title":    { "title : index" }
            #TODO: define your fields here
        }
    }
}

# Name of index
sc_index_name = 'ir_test_sc'

es = Elasticsearch(['http://localhost:9200'], http_auth=('elastic', '7s-sKB6EPzut8lMdf6bF'))

# Delete index if one does exist
if es.indices.exists(index=sc_index_name):
    es.indices.delete(index=sc_index_name)

# Create index
es.indices.create(index=sc_index_name, body=sc_mapping)

load_data_to_elastic(es,data_tmp,sc_index_name)

def get_suggestions(text , index_name):
    resp = es.search(index=index_name,suggest={
        "text": text,
        "simple_phrase": {
            "phrase": {
                "smoothing" : 1,
                "field": "title.trigram",
                "size": 1,
                "confidence": 1,
                "real_word_error_likelihood": 0.95,
                "max_errors" : 1,
                "direct_generator": [

                ]
            }
        }
    },size=0)
    return dict(resp)

texts = [
    "لیک برتر فوطبال",
    "تورنومنت شش جانبه",
    "طبعیض نژادی",
    "اردوی طیم امیذ",
    "جام ملب های آشیا",
    "کنره سیاسی آمریکا",
    "انغلاب اشلامی ایران",
    "فدراصیون فوتبال ایران",
    "لایهه مجلص خبرگان",
    "نحبگان دانشگاهی",
    "نمایند مجل",
    "فضاسازی کازب",
    "صلاح ایرادات شورای تگهبان",
    "تهریف های تاریخی",
    "قلیت های دینی",
    "تدارکان لازم",
]

for text in texts:
    print(get_suggestions(text,sc_index_name )['suggest']['simple_phrase'])
    print("========================")



