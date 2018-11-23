from elastic.config import es
from elastic.get_documents import get_documents_by_id
from elasticsearch import Elasticsearch

from pprint import pprint


def fetch_data( index, filterByTerm, removeKeywords, categories=[], remove_duplicate=True, convert_to_ascii=True, preprocess=False, es_doc_type="page", es=None):

    if es == None:
        es = Elasticsearch("http://localhost:9200")

    print index
    print categories

    MAX_WORDS = 1000

    index = index
    doctype = es_doc_type
    mapping = {"timestamp":"retrieved", "text":"text", "html":"html", "tag":"tag", "query":"query"}

    records = []

    fields = ["url", "tag", "text", "description", "image_url", "title"]
    query = {
        "query": {
            "match_all": {}
        },
        "fields": fields,
        "size": 100000000
    }
    if filterByTerm != "":
        query = "(text:" + filterByTerm + ")"
        query = {
            "query": {
                "query_string": {
                    "query": query
                }
            },
            "fields": fields,
            "size": 100000000
        }

    if len(categories) > 0:
        query = {
            "query" : {
                "filtered" : {
                    "filter" : {
                        "exists" : { "field" : "tag" }
                    }
                }
            },
            "fields": fields,
            #size": 3000
            "size": 100000000
        }

    res = es.search(body=query,
                    index=index,
                    doc_type=doctype, request_timeout=2000)

    if res['hits']['hits']:
        hits = res['hits']['hits']

    for hit in hits:
        record = {}
        if not hit.get('fields') is None:
            record = hit['fields']
            record['id'] =hit['_id']
            records.append(record)

    del res
    del hits

    result = {}
    labels = []
    urls = []
    snippet = []
    title = []
    image_url = []
    text = []
    topic_count = {}
    dup_count = 0
    i_title=0
    for rec in records:
        dup = -1
        try:
            dup = text.index(rec["text"][0])
        except KeyError:
            pprint(rec)
        except ValueError:
            dup = -1

        if remove_duplicate:
            if dup != -1:
                dup_count = dup_count + 1
                print rec["id"], " ", urls[dup]
                continue

        try:
            topic_name = ",".join(rec["tag"])
        except KeyError:
            topic_name = "Neutral"

        if (topic_name in categories) or len(categories) == 0:
            labels.append(topic_name)
            if removeKeywords:
                if rec.get("text") is not None:
                    textTemp = preprocessF(removeKeywords, rec["text"][0][0:MAX_WORDS])
                    text.append(textTemp)
                else:
                    continue
            #if preprocess:
                #text.append(preprocess(rec["text"][0])[0:MAX_WORDS])
            else:
                if rec.get("text") is not None:
                    text.append(rec["text"][0][0:MAX_WORDS])
                else:
                    continue
            urls.append(rec["url"][0])
            if not rec.get('description') is None:
                snippet.append(" ".join(rec['description'][0].split(" ")[0:20]))
            else:
                snippet.append("")
            if not rec.get('image_url') is None:
                image_url.append(rec['image_url'][0])
            else:
                image_url.append("")
            if not rec.get('title') is None:
                title.append(rec['title'][0])
            else:
                title.append("")


            count = topic_count.get(topic_name)
            if count is None:
                count = 1
            else:
                count = count + 1
            topic_count[topic_name] = count

    if remove_duplicate:
        print "\n\nDuplicates found = ", dup_count

    result["labels"] = labels
    result["data"] = text
    result["urls"] = urls
    result["label_count"] = topic_count
    result["snippet"] = snippet
    result["title"] =title
    result["image_url"] = image_url
    return result

def preprocess(text, convert_to_ascii=True):
    # Remove unwanted chars and new lines
    text = text.lower().replace(","," ").replace("__"," ").replace("(", " ").replace(")", " ").replace("[", " ").replace("]", " ").replace(".", " ").replace("/", " ").replace("\\", " ").replace("_", " ").replace("#", " ").replace("-", " ").replace("+", " ").replace("%", " ").replace(";", " ").replace(":", " ").replace("'", " ").replace("\""," ").replace("^", " ")
    text = text.replace("\n"," ")

    if convert_to_ascii:
        # Convert to ascii
        ascii_text = []
        for x in text.split(" "):
            try:
                ascii_text.append(x.encode('ascii', 'ignore'))
            except:
                continue

        text = " ".join(ascii_text)

    preprocessed_text = " ".join([word.strip() for word in text.split(" ") if len(word.strip()) > 2 and (word.strip() != "") and (isnumeric(word.strip()) == False) and notHtmlTag(word.strip()) and notMonth(word.strip())])

    return preprocessed_text

def preprocessF(removeKeywords, text):
    # Remove unwanted chars and new lines
    text = text.lower().replace(","," ").replace("__"," ").replace("(", " ").replace(")", " ").replace("[", " ").replace("]", " ").replace(".", " ").replace("/", " ").replace("\\", " ").replace("_", " ").replace("#", " ").replace("-", " ").replace("+", " ").replace("%", " ").replace(";", " ").replace(":", " ").replace("'", " ").replace("\""," ").replace("^", " ")
    text = text.replace("\n"," ")
    convert_to_ascii = True
    if convert_to_ascii:
        # Convert to ascii
        ascii_text = []
        for x in text.split(" "):
            try:
                ascii_text.append(x.encode('ascii', 'ignore'))
            except:
                continue

        text = " ".join(ascii_text)
    print text
    preprocessed_text = " ".join([word.strip() for word in text.split(" ") if len(word.strip()) > 2 and (word.strip() != "") and (isnumeric(word.strip()) == False) and notHtmlTag(word.strip()) and notMonth(word.strip()) and notSelectedKeywords(word.strip(), removeKeywords)])
    return preprocessed_text

def notSelectedKeywords(word, removeKeywords):
    selectedKeywords_tags = removeKeywords #["arizona", "california", "colorado", "florida","georgia", "iowa","illinois", "missouri","nevada", "new", "hampshire", "york", "north", "carolina", "ohio", "rhode", "island", "pennsylvania","texas", "virginia", "wisconsin","national","global", "project","punditFact","terms","punditfact", "people","pants", "fire"]

    if word in selectedKeywords_tags:
        return False

    return True

def notHtmlTag(word):
    html_tags = ["http", "html", "img", "images", "image", "index"]

    for tag in html_tags:
        if (tag in word) or (word in ["url", "com", "www", "www3", "admin", "backup", "content"]):
            return False

    return True

def notMonth(word):
    month_tags = ["jan", "january", "feb", "february","mar", "march","apr", "april","may", "jun", "june", "jul", "july", "aug", "august","sep", "sept", "september","oct","october","nov","november","dec", "december","montag", "dienstag", "mittwoch", "donnerstag", "freitag", "samstag", "sontag"]

    if word in month_tags:
        return False

    return True

def isnumeric(s):
    # Check if string is a numeric
    try:
        int(s)
        return True
    except ValueError:
        try:
            long(s)
            return True
        except ValueError:
            return False
