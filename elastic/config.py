'''
provides access to elasticsearch server

es_server - the name of the endpoint
es - an Elasticsearch instance connected to es_server
'''

from elasticsearch import Elasticsearch
from os import environ
import certifi

if environ.get('ELASTICSEARCH_SERVER'):
    use_ssl = False
    es_server = environ['ELASTICSEARCH_SERVER']
    if "https" in es_server:
        use_ssl=True
else:
    es_server = 'http://localhost:9200/'

print 'ELASTICSEARCH_SERVER ', es_server

if environ.get('ELASTICSEARCH_USER'):
    es_user = environ['ELASTICSEARCH_USER']
else: 
    es_user = ""

print 'ELASTICSEARCH_USER ', es_user

if environ.get('ELASTICSEARCH_PASSWD'):
    es_passwd = environ['ELASTICSEARCH_PASSWD']
else:
    es_passwd = ""

if es_user:
    if use_ssl:
        es = Elasticsearch([es_server], http_auth=(es_user, es_passwd), use_ssl=True, verify_certs=True, ca_certs=certifi.where(), timeout=100)
    else:
        es = Elasticsearch([es_server], http_auth=(es_user, es_passwd), timeout=100)
else:
    es = Elasticsearch([es_server])

if environ.get('ELASTICSEARCH_DOC_TYPE'):
    es_doc_type = environ['ELASTICSEARCH_DOC_TYPE']
else:
    es_doc_type = 'page'


