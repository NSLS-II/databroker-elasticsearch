from elasticsearch import Elasticsearch


def es(host="127.0.0.1"):
    e = Elasticsearch(hosts=[host])
    # XXX: maybe need to make index here?
    return e
