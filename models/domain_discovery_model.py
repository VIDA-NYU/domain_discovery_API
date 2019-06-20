import time
import calendar
import shutil
from math import fsum
from datetime import datetime
from dateutil import tz
from sets import Set
from itertools import product
from signal import SIGTERM
import shlex
from pprint import pprint

from sklearn.decomposition import PCA
from sklearn.manifold import TSNE
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer

import scipy as sp
import numpy as np
from random import random, randint, sample

from subprocess import Popen
from subprocess import PIPE

import linecache
from sys import exc_info
from os import chdir, listdir, environ, makedirs, rename, chmod, walk, remove
from os.path import isfile, join, exists, isdir

from elasticsearch import Elasticsearch

from seeds_generator.download import callDownloadUrls, getImage, getDescription
from seeds_generator.runSeedFinder import RunSeedFinder
from seeds_generator.concat_nltk import get_bag_of_words
from elastic.get_config import get_available_domains, get_mapping, get_tag_colors
from elastic.search_documents import get_context, term_search, search, range_search, multifield_term_search, multifield_query_search, field_missing, field_exists
from elastic.misc_queries import exec_query, random_sample
from elastic.add_documents import add_document, update_document, delete_document, refresh
from elastic.get_mtermvectors import getTermStatistics, getTermFrequency
from elastic.get_documents import (get_most_recent_documents, get_documents,
    get_all_ids, get_more_like_this, get_documents_by_id,
    get_plotting_data)
from elastic.aggregations import get_significant_terms, get_unique_values
from elastic.create_index import create_index, create_terms_index, create_config_index
from elastic.load_config import load_config
from elastic.delete_index import delete_index
from elastic.config import es, es_doc_type, es_server
from elastic.delete import delete

from ranking import tfidf, rank, extract_terms, word2vec, get_bigrams_trigrams

from online_classifier.online_classifier import OnlineClassifier
from online_classifier.tfidf_vector import tfidf_vectorizer
from online_classifier.tf_vector import tf_vectorizer


#from topik import read_input, tokenize, vectorize, run_model, visualize, TopikProject

from concurrent.futures import ThreadPoolExecutor as Pool

import urllib2

MAX_TEXT_LENGTH = 3000
MAX_TERM_FREQ = 2
MAX_LABEL_PAGES = 2000
MAX_SAMPLE_PAGES = 500

class DomainModel(object):

  w2v = word2vec.word2vec(from_es=False)

  def __init__(self, path=""):
    self._es = None
    self._all = 100000
    self._termsIndex = "ddt_terms"
    self._pagesCapTerms = 100
    self._capTerms = 500
    self.projectionsAlg = {'Group by Similarity': self.pca,
                           'Group by Correlation': self.tsne
                           # 'K-Means': self.kmeans,
                         }
    self._predefined_tags = ["Deep Crawl"];

    create_config_index()
    create_terms_index()

    self._mapping = {"url":"url", "timestamp":"retrieved", "text":"text", "html":"html", "tag":"tag", "query":"query", "domain":"domain", "title":"title", "description":"description"}
    self._domains = None
    self._onlineClassifiers = {}
    self._classifiersCrawler = {}
    self._pos_tags = ['NN', 'NNS', 'NNP', 'NNPS', 'FW', 'JJ']
    self._path = path

    self.results_file = open("results.txt", "w")

    self.pool = Pool(max_workers=3)
    self.seedfinder = RunSeedFinder()
    self.runningSeedFinders={}
    self.extractTermsVectorizer = {}

    self._initACHE()

  def _encode(self, url):
    return urllib2.quote(url).replace("/", "%2F")

  def _esInfo(self, domainId):
    es_info = {
      "activeDomainIndex": self._domains[domainId]['index'],
      "docType": self._domains[domainId]['doc_type']
    }
    if not self._domains[domainId].get("mapping") is None:
      es_info["mapping"] = self._domains[domainId]["mapping"]
    else:
      es_info["mapping"] = self._mapping
    return es_info

  def _initACHE(self):

    with open(self._path+"/ache.yml","w") as fw:
      fw.write("#" + "\n")
      fw.write("# Example of configuration for running a Focused Crawl" + "\n")
      fw.write("#" + "\n")
      fw.write("target_storage.use_classifier: true" + "\n")
      fw.write("target_storage.store_negative_pages: true" + "\n")
      fw.write("target_storage.data_format.type: ELASTICSEARCH" + "\n")
      fw.write("target_storage.data_format.elasticsearch.rest.hosts:" + "\n")
      fw.write("  - http://" + es_server + ":9200" + "\n")
      fw.write("target_storage.english_language_detection_enabled: false" + "\n")

      fw.write("link_storage.max_pages_per_domain: 1000" + "\n")
      fw.write("link_storage.link_strategy.use_scope: false" + "\n")
      fw.write("link_storage.link_strategy.outlinks: true" + "\n")
      fw.write("link_storage.link_strategy.backlinks: false" + "\n")
      fw.write("link_storage.link_classifier.type: LinkClassifierBaseline" + "\n")
      fw.write("link_storage.online_learning.enabled: true" + "\n")

      fw.write("link_storage.online_learning.type: FORWARD_CLASSIFIER_BINARY" + "\n")
      fw.write("link_storage.online_learning.learning_limit: 1000" + "\n")
      fw.write("link_storage.link_selector: TopkLinkSelector" + "\n")
      fw.write("link_storage.scheduler.host_min_access_interval: 5000" + "\n")
      fw.write("link_storage.scheduler.max_links: 10000" + "\n")

      fw.write("crawler_manager.downloader.user_agent.name: ACHE" + "\n")
      fw.write("crawler_manager.downloader.user_agent.url: https://github.com/ViDA-NYU/ache" + "\n")
      fw.write("crawler_manager.downloader.valid_mime_types:" + "\n")
      fw.write(" - text/html" + "\n")


  def setPath(self, path):
    self._path = path

  def getPath(self, path):
    return self._path

  def setCrawlerModel(self, crawlerModel):
    self._crawlerModel = crawlerModel

  def getStatus(self, session):
    status = {}

    runningCrawlers = self._crawlerModel.runningCrawlers

    if len(runningCrawlers.keys()) > 0:
      status["Crawler"] = []
      for k,v in runningCrawlers.items():
        for type, crawler_info in v.items():
          crawler_status = self._crawlerModel.getStatus(type,session)
          if crawler_status.get("crawlerState") == "RUNNING":
            status["Crawler"].append({"domain": crawler_info["domain"], "status": crawler_info["status"], "description": type})
          elif crawler_status.get("crawlerState") == "STOPPING":
            status["Crawler"].append({"domain": crawler_info["domain"], "status": "Terminating", "description":type})
          elif crawler_status.get("crawlerState") == "TERMINATED":
            self._crawlerModel.crawlerStopped(type, session)
            break

    if len(self.runningSeedFinders.keys()) > 0:
      status["SeedFinder"] = []
      for k,v in self.runningSeedFinders.items():
        status["SeedFinder"].append({"domain": v["domain"], "status": v["status"], "description": v["description"]})

    return status

  def stopProcess(self, process, process_info):
    print "Stop Process ",process," ",process_info
    if process == "Crawler":
      runningCrawlers = self._crawlerModel.runningCrawlers
      session = {"domainId": runningCrawlers.keys()[0]}
      self._crawlerModel.stopCrawler(process_info.get('description'), session)
    elif process == "SeedFinder":
      query = process_info["description"].replace('Query: ', '')
      self.stopSeedFinder(query)

    message = "Stopped process " + process
    description = process_info.get("description")
    if not description is None:
      message = message + " for " + description

    return message

  def getAvailableProjectionAlgorithms(self):
    return [{'name': key} for key in self.projectionsAlg.keys()]

  def getAvailablePageRetrievalCriteria(self):
    return [{'name': key} for key in self.pageRetrieval.keys()]

  def getAvailableDomains(self):
    """
    List the domains as saved in elasticsearch.

    Parameters:
        None

    Returns:
       array: [
           {'id': domainId, 'name': domainName, 'creation': epochInSecondsOfFirstDownloadedURL},
           ...
           ]

    """
    # Initializes elastic search.
    self._es = es

    self._domains = get_available_domains(self._es)

    return \
    [{'id': k, 'name': d['domain_name'], 'creation': d['timestamp'], 'index': d['index'], 'doc_type': d['doc_type']} for k, d in self._domains.items()]

  def getAvailableTLDs(self, session):
    """ Return all top level domains for the selected domain.

    Parameters:
        session (json): Should contain the domainId

    Returns:
        json: {<domain>: <number of pages for the domain>}

    """
    es_info = self._esInfo(session['domainId'])

    unique_tlds = {}

    for k, v in get_unique_values('domain.exact', None, self._all, es_info['activeDomainIndex'], es_info['docType'], self._es).items():
      if "." in k:
        unique_tlds[k] = v

    return unique_tlds

  def getAvailableQueries(self, session):
    """ Return all queries for the selected domain.

    Parameters:
        session (json): Should contain the domainId

    Returns:
        json: {<query>: <number of pages for the query>}

    """
    es_info = self._esInfo(session['domainId'])
    queries = get_unique_values('query', None, self._all, es_info['activeDomainIndex'], es_info['docType'], self._es)

    return queries

  def getAvailableTags(self, session):
    """ Return all tags for the selected domain.

    Parameters:
        session (json): Should contain the domainId

    Returns:
        json: {<tag>: <number of pages for the tag>}

    """
    es_info = self._esInfo(session['domainId'])

    query = {
      "query" : {
        "filtered" : {
          "filter" : {
            "missing" : { "field" : "tag"}
          }
        }
      }
    }
    tags_neutral = self._es.count(es_info['activeDomainIndex'], es_info['docType'],body=query)
    unique_tags = {"Neutral": tags_neutral['count']}

    tags = get_unique_values('tag', None, self._all, es_info['activeDomainIndex'], es_info['docType'], self._es)
    for tag, num in tags.iteritems():
      if tag != "":
        if unique_tags.get(tag) is not None:
          unique_tags[tag] = unique_tags[tag] + num
        else:
          unique_tags[tag] = num
      else:
        unique_tags["Neutral"] = unique_tags["Neutral"] + 1

    for tag in self._predefined_tags:
      if unique_tags.get(tag) is None:
        unique_tags[tag] = 0

    return unique_tags

  def getResultModel(self, session):
    es_info = self._esInfo(session['domainId'])

    return self.predictData(session)

  def getAvailableModelTags(self, session):
    es_info = self._esInfo(session['domainId'])

    return self.predictUnlabeled(session)

  def getAvailableCrawledData(self, session):
    es_info = self._esInfo(session['domainId'])

    unique_tags = {}

    query = {
      "query" : {
        "term" : {
          "isRelevant": "relevant"
        }
      }
    }
    crawlData = self._es.count(es_info['activeDomainIndex'], es_info['docType'],body=query, request_timeout=30)
    count = crawlData['count']
    unique_tags["CD Relevant"] = count

    query = {
      "query" : {
        "term" : {
          "isRelevant": "irrelevant"
        }
      }
    }
    crawlData = self._es.count(es_info['activeDomainIndex'], es_info['docType'],body=query, request_timeout=30)
    count = crawlData['count']
    unique_tags["CD Irrelevant"] = count

    return unique_tags


  def getPagesSummaryDomain(self, opt_ts1 = None, opt_ts2 = None, opt_applyFilter = False, session = None):

    """ Returns number of pages downloaded between opt_ts1 and opt_ts2 for active domain.
    If opt_applyFilter is True, the summary returned corresponds
    to the applied pages filter defined previously in @applyFilter. Otherwise the returned summary
    corresponds to the entire dataset between ts1 and ts2.

    Parameters:
        opt_ts1 (long): start time from when pages need to be returned. Unix epochs (seconds after 1970).

        opt_ts2 (long): start time from when pages need to be returned. Unix epochs (seconds after 1970).

        opt_applyFiler (bool): Apply filtering to the pages

        session (json): session information

    Returns:
        json: {'Positive': {'Explored': explored, 'Exploited': exploited, 'Boosted': boosted},
               'Negative': {'Explored': numExploredPages, 'Exploited': numExploitedPages},...}
    """
    es_info = self._esInfo(session['domainId'])

    # If ts1 not specified, sets it to -Infinity.
    if opt_ts1 is None:
      now = time.gmtime(0)
      opt_ts1 = float(calendar.timegm(now))
    else:
      opt_ts1 = float(opt_ts1)

    # If ts2 not specified, sets it to now.
    if opt_ts2 is None:
      now = time.gmtime()
      opt_ts2 = float(calendar.timegm(now))
    else:
      opt_ts2 = float(opt_ts2)
    total_results = []
    total_results = get_most_recent_documents(2000, es_info['mapping'], [es_info['mapping']['url'], es_info['mapping']["tag"]],
                                      None, es_info['activeDomainIndex'], es_info['docType'],  \
                                      self._es)
    if opt_applyFilter and session['filter'] != "":
      #TODO(Sonia):results based in multi queries
      results = self._getPagesQuery(session)

      #results = get_most_recent_documents(session['pagesCap'], es_info['mapping'], ["url", es_info['mapping']["tag"]],
                                          #session['filter'], es_info['activeDomainIndex'], es_info['docType'],  \
                                          #self._es)

    else:
      #results = get_most_recent_documents(2000, es_info['mapping'], ["url", es_info['mapping']["tag"]],
                                        #session['filter'], es_info['activeDomainIndex'], es_info['docType'],  \
                                        #self._es)
      results = \
      range_search(es_info['mapping']["timestamp"], opt_ts1, opt_ts2, [es_info['mapping']['url'],es_info['mapping']['tag']], True, session['pagesCap'], es_index=es_info['activeDomainIndex'], es_doc_type=es_info['docType'], es=self._es)


    relevant = 0
    irrelevant = 0
    neutral = 0
    otherTags = 0
    total_relevant = 0
    total_irrelevant = 0
    total_neutral = 0

    for res_total in total_results:
        try:
          total_tags = res_total[es_info['mapping']['tag']]
          if 'Irrelevant' in res_total[es_info['mapping']['tag']]:
            total_irrelevant = total_irrelevant + 1
          else:
            # Page has tags Relevant or custom.
            if "" not in total_tags:
                if 'Relevant' in total_tags:
                    total_relevant = total_relevant + 1
            else:
              total_neutral = total_neutral + 1
        except KeyError:
          # Page does not have tags.
          total_neutral = total_neutral + 1

    for res in results:
        try:
          tags = res[es_info['mapping']['tag']]
          if 'Irrelevant' in res[es_info['mapping']['tag']]:
            irrelevant = irrelevant + 1
          else:
            # Page has tags Relevant or custom.
            if "" not in tags:
                if 'Relevant' in tags:
                    relevant = relevant + 1
                else:
                    otherTags = otherTags + 1
            else:
                neutral = neutral + 1
        except KeyError:
          # Page does not have tags.
          neutral = neutral + 1 #1



    return { \
      'Relevant': relevant,
      'Irrelevant': irrelevant,
      'Neutral': neutral,
      'OtherTags': otherTags,
      'TotalRelevant': total_relevant,
      'TotalIrrelevant': total_irrelevant,
      'TotalNeutral': total_neutral
    }


  def _setPagesCountCap(self, pagesCap):
    self._pagesCap = int(pagesCap)

  # Boosts set of pages: domain exploits outlinks for the given set of pages in active domain.
  def boostPages(self, pages):
    # TODO(Yamuna): Issue boostPages on running domain defined by active domainId.
    i = 0
    print 3 * '\n', 'boosted pages', str(pages), 3 * '\n'

  # Fetches snippets for a given term.
  def getTermSnippets(self, term, session):
    es_info = self._esInfo(session['domainId'])

    #tags = get_documents(term, 'term', ['tag'], es_info['activeDomainIndex'], 'terms', self._es)


    s_fields = {
      "term": term,
      "index": es_info['activeDomainIndex'],
      "doc_type": es_info['docType'],
    }

    results = multifield_term_search(s_fields, 0, self._capTerms, ['tag'], self._termsIndex, 'terms', self._es)
    tags = results["results"]

    tag = []
    if tags:
      tag = tags[0]['tag'][0].split(';')

    return {'term': term, 'tags': tag, 'context': get_context(term.split('_'), es_info['mapping']['text'], 500, es_info['activeDomainIndex'], es_info['docType'],  self._es)}

  # Delete terms from term window and from the ddt_terms index
  def deleteTerm(self,term, session):
    es_info = self._esInfo(session['domainId'])
    delete([term+'_'+es_info['activeDomainIndex']+'_'+es_info['docType']], self._termsIndex, "terms", self._es)

  def getAnnotatedTerms(self, session):
    es_info = self._esInfo(session['domainId'])

    s_fields = {
      "index": es_info['activeDomainIndex'],
      "doc_type": es_info['docType']
    }

    results = multifield_term_search(s_fields, 0, self._all, ['tag','term'], self._termsIndex, 'terms', self._es)

    hits = results["results"]
    terms = {}
    for hit in hits:
      term = hit['term'][0]
      terms[term] = {'tag':hit['tag'][0]}

    return terms

  # Add domain
  def addDomain(self, index_name):

    create_index(index_name, es=self._es)

    fields = index_name.lower().split(' ')
    index = '_'.join([item for item in fields if item not in ''])
    index_name = ' '.join([item for item in fields if item not in ''])
    entry = { "domain_name": index_name.title(),
              "index": index,
              "doc_type": "page",
              "timestamp": datetime.utcnow()
            }

    load_config([entry])

    self._crawlerModel.updateDomains()

  # Delete domain
  def delDomain(self, domains):

    for index in domains.values():
      # Delete Index
      delete_index(index, self._es)
      # Delete terms tagged for the index
      ddt_terms_keys = [doc["id"] for doc in term_search("index", [index], 0, self._all, ["term"], "ddt_terms", "terms", self._es)["results"]]
      delete_document(ddt_terms_keys, "ddt_terms", "terms", self._es)

      #Delete all the data for the index
      data_dir = self._path + "/data/"
      data_domain  = data_dir + index
      if isdir(data_domain):
        shutil.rmtree(data_domain)

    # Delete indices from config index
    delete_document(domains.keys(), "config", "domains", self._es)

    self._crawlerModel.updateDomains()

  def updateColors(self, session, colors):
    es_info = self._esInfo(session['domainId'])

    entry = {
      session['domainId']: {
        "colors": colors["colors"],
        "index": colors["index"]
      }
    }

    update_document(entry, "config", "tag_colors", self._es)

  def getTagColors(self, domainId):
    tag_colors = get_tag_colors(self._es).get(domainId)

    colors = None
    if tag_colors is not None:
      colors = {"index": tag_colors["index"]}
      colors["colors"] = {}
      for color in tag_colors["colors"]:
        fields  = color.split(";")
        colors["colors"][fields[0]] = fields[1]

    return colors

#######################################################################################################
# Acquire Content
#######################################################################################################

  def queryWeb(self, terms, max_url_count = 100, session = None):
    """ Issue query on the web: results are stored in elastic search, nothing returned here.

    Parameters:
        terms (string): Search query string
        max_url_count (int): Number of pages to query. Maximum allowed = 100
        session (json): should have domainId

    Returns:
        None

    """
    es_info = self._esInfo(session['domainId'])

    chdir(environ['DD_API_HOME']+'/seeds_generator')

    if(int(session['pagesCap']) <= max_url_count):
      top = int(session['pagesCap'])
    else:
      top = max_url_count

    if 'GOOG' in session['search_engine']:
      comm = 'java -cp target/seeds_generator-1.0-SNAPSHOT.jar GoogleSearch -t ' + str(top) + \
             ' -q \'' + terms + '\'' + \
             ' -i ' + es_info['activeDomainIndex'] + \
             ' -d ' + es_info['docType'] + \
             ' -s ' + es_server

    elif 'BING' in session['search_engine']:
      comm = 'java -cp target/seeds_generator-1.0-SNAPSHOT.jar BingSearch -t ' + str(top) + \
             ' -q \'' + terms + '\'' + \
             ' -i ' + es_info['activeDomainIndex'] + \
             ' -d ' + es_info['docType'] + \
             ' -s ' + es_server

    p=Popen(comm, shell=True, stdout=PIPE)
    output, errors = p.communicate()

    print "\n\n\n QUERY WEB OUTPUT \n", "\n",output,"\n\n\n"
    print "\n\n\n QUERY WEB ERRORS \n", errors,"\n\n\n"

    num_pages = self._getNumPagesDownloaded(output)

    return {"pages":num_pages}

  def _getNumPagesDownloaded(self, output):
    index = output.index("Number of results:")
    n_pages = output[index:]
    n = int(n_pages.split(":")[1])
    return n

  def uploadUrls(self, urls_str, tag, session):
    """ Download pages corresponding to already known set of domain URLs

    Parameters:
        urls_str (string): Space separated list of URLs
        session (json): should have domainId

    Returns:
        number of pages downloaded (int)

    """
    es_info = self._esInfo(session['domainId'])

    output = callDownloadUrls("uploaded", None, urls_str, tag, es_info)

    return output

  def getForwardLinks(self, urls, session):
    """ The content can be extended by crawling the given pages one level forward. The assumption here is that a relevant page will contain links to other relevant pages.

    Parameters:
        urls (list): list of urls to crawl forward
        session (json): should have domainId

    Return:
        None (Results are downloaded into elasticsearch)
    """
    session['pagesCap']='100'
    es_info = self._esInfo(session['domainId'])

    results = field_exists("crawled_forward", [es_info['mapping']['url'], "crawled_forward"], self._all, es_info['activeDomainIndex'], es_info['docType'], self._es)
    already_crawled = [result[es_info["mapping"]["url"]][0] for result in results if result["crawled_forward"][0] == 1]
    not_crawled = list(Set(urls).difference(already_crawled))
    results = get_documents(not_crawled, es_info["mapping"]['url'], [es_info["mapping"]['url']], es_info['activeDomainIndex'], es_info['docType'], self._es)
    not_crawled_urls = [results[url][0][es_info["mapping"]["url"]][0] for url in not_crawled]
    chdir(environ['DD_API_HOME']+'/seeds_generator')
    comm = "java -cp target/seeds_generator-1.0-SNAPSHOT-jar-with-dependencies.jar StartCrawl -c forward"\
           " -u \"" + ",".join(not_crawled_urls) + "\"" + \
           " -t " + session["pagesCap"] + \
           " -i " + es_info['activeDomainIndex'] + \
           " -d " + es_info['docType'] + \
           " -s " + es_server

    p=Popen(comm, shell=True, stderr=PIPE)
    output, errors = p.communicate()
    print output
    print errors

  # Crawl backward
  def getBackwardLinks(self, urls, session):
    """ The content can be extended by crawling the given pages one level back to the pages that link to them. The assumption here is that a page containing the link to the given relevant page will contain links to other relevant pages.

    Parameters:
        urls (list): list of urls to crawl backward
        session (json): should have domainId

    Return:
        None (Results are downloaded into elasticsearch)

    """
    session['pagesCap']='100'
    es_info = self._esInfo(session['domainId'])

    results = field_exists("crawled_backward", [es_info['mapping']['url']], self._all, es_info['activeDomainIndex'], es_info['docType'], self._es)
    already_crawled = [result[es_info["mapping"]["url"]][0] for result in results]
    not_crawled = list(Set(urls).difference(already_crawled))
    results = get_documents(not_crawled, es_info["mapping"]['url'], [es_info["mapping"]['url']], es_info['activeDomainIndex'], es_info['docType'], self._es)
    not_crawled_urls = [results[url][0][es_info["mapping"]["url"]][0] for url in not_crawled]

    chdir(environ['DD_API_HOME']+'/seeds_generator')

    comm = "java -cp target/seeds_generator-1.0-SNAPSHOT-jar-with-dependencies.jar StartCrawl -c backward"\
           " -u \"" + ",".join(not_crawled_urls) + "\"" + \
           " -t " + session["pagesCap"] + \
           " -i " + es_info['activeDomainIndex'] + \
           " -d " + es_info['docType'] + \
           " -s " + es_server

    p=Popen(comm, shell=True, stderr=PIPE)
    output, errors = p.communicate()
    print output
    print errors

  def runSeedFinder(self, terms, session):
    """ Execute the SeedFinder witht the specified terms. The details of the url results of the SeedFinder
    are uploaded into elasticsearch.

    Parameters:
        terms (str): terms for the inital query

        session (json): Should contain the domainId

    Returns:
        None
    """
    domainId = session['domainId']
    es_info = self._esInfo(domainId);


    # Execute SeedFinder in a new thread
    if self.runningSeedFinders.get(terms) is not None:
      if not self.runningSeedFinders[terms]['shouldTerminate']:
        return self.runningSeedFinders[terms]['status']

    data_dir = self._path + "/data/"
    data_domain  = data_dir + es_info['activeDomainIndex']

    domainmodel_dir = data_domain + "/models/"

    self.runningSeedFinders[terms] = {"domain": self._domains[domainId]['domain_name'], "status": "Starting", "description":"Query: "+terms, 'shouldTerminate': False}

    if (not isfile(domainmodel_dir+"pageclassifier.model")):
      self.runningSeedFinders[terms]["status"] = "Creating Model"
      self._crawlerModel.createModel(session, zip=False)

    print "\n\n\n RUN SEED FINDER",terms,"\n\n\n"

    self.runningSeedFinders[terms]["status"] = "Starting"

    if not self.runningSeedFinders[terms]['shouldTerminate']:
      p = self.pool.submit(self.seedfinder.execSeedFinder, terms, self._path, self._updateSeedFinderStatus, self._seedfinderShouldTerminate, es_info)
      self.runningSeedFinders[terms]["process"] = p
      p.add_done_callback(self._seedfinderCompleted)
    else:
      del self.runningSeedFinders[terms]
      return "Terminated"

    return "Starting"

  def stopSeedFinder(self, terms):
    self.runningSeedFinders[terms]['status'] = "Terminating"
    self.runningSeedFinders[terms]['shouldTerminate'] = True
    if not self.runningSeedFinders[terms].get('terminate_process') is None:
      self.runningSeedFinders[terms]['terminate_process'].kill()

    print "\n\n\nSeedFinder terminating for ", terms," \n\n\n"

    return "Terminating"

  #Method to update the seed finder status
  def _updateSeedFinderStatus(self, terms, field, value):
    self.runningSeedFinders[terms][field] = value

  def _seedfinderShouldTerminate(self, terms):
    return self.runningSeedFinders[terms]['shouldTerminate']

  def _seedfinderCompleted(self, p):
    for k,v in self.runningSeedFinders.items():
      if v["process"] == p:
        if self.runningSeedFinders[k]['shouldTerminate']:
          del self.runningSeedFinders[k]
          print "\n\n\nSeedFinder terminated for ", k," \n\n\n"
        else:
          print "\n\n\n SeedFinder COMPLETED for ", k, "\n\n\n"
          if not self.runningSeedFinders[k]['status'] == "No relevant results":
            self.runningSeedFinders[k]['status'] = "Completed"
        break

#######################################################################################################
# Annotate Content
#######################################################################################################

  def setPagesTag(self, pages, tag, applyTagFlag, session):
    """ Tag the pages with the given tag which can be a custom tag or 'Relevant'/'Irrelevant' which indicate relevance or irrelevance to the domain of interest. Tags help in clustering and categorizing the pages. They also help build computational models of the domain.

    Parameters:
        pages (urls): list of urls to apply tag
        tag (string): custom tag, 'Relevant', 'Irrelevant'
        applyTagFlag (bool): True - Add tag, False - Remove tag
        session (json): Should contain domainId

    Returns:
       Returns string "Completed Process"

    """

    es_info = self._esInfo(session['domainId'])

    entries = {}
    results = get_documents(pages, es_info['mapping']['url'], [es_info['mapping']['tag']], es_info['activeDomainIndex'], es_info['docType'],  self._es)

    if applyTagFlag and len(results) > 0:
      print '\n\napplied tag ' + tag + ' to pages' + str(pages) + '\n\n'

      for page in pages:
        if not results.get(page) is None:
          # pages to be tagged exist
          records = results[page]
          for record in records:
            entry = {}
            if record.get(es_info['mapping']['tag']) is None:
              # there are no previous tags
              entry[es_info['mapping']['tag']] = [tag]
              entry["unsure_tag"] = 0
              entry["label_pos"] = 0
              entry["label_neg"] = 0
            else:
              tags = record[es_info['mapping']['tag']]
              if len(tags) != 0 and ("Neutral" not in tags):
                # previous tags exist
                if not tag in tags:
                  # append new tag
                  tags.append(tag)
                  entry[es_info['mapping']['tag']] = tags
                  entry["unsure_tag"] = 0
                  entry["label_pos"] = 0
                  entry["label_neg"] = 0

                  self._removeClassifierSample(session['domainId'], record['id'])
              else:
                # add new tag
                entry[es_info['mapping']['tag']] = [tag]
                entry["unsure_tag"] = 0
                entry["label_pos"] = 0
                entry["label_neg"] = 0

            if entry:
                  entries[record['id']] =  entry

    elif len(results) > 0:
      print '\n\nremoved tag ' + tag + ' from pages' + str(pages) + '\n\n'

      for page in pages:
        if not results.get(page) is None:
          records = results[page]
          for record in records:
            entry = {}
            if not record.get(es_info['mapping']['tag']) is None:
              tags = record[es_info['mapping']['tag']]
              if tag in tags:
                tags.remove(tag)
                entry[es_info['mapping']['tag']] = tags
                entries[record['id']] = entry


    if entries:
      update_try = 0
      while (update_try < 10):
        try:
          update_document(entries, es_info['activeDomainIndex'], es_info['docType'], self._es)
          break
        except:
          update_try = update_try + 1

      if (session['domainId'] in self._onlineClassifiers) and (not applyTagFlag) and (tag in ["Relevant", "Irrelevant"]):
        self._onlineClassifiers.pop(session['domainId'])

    return "Completed Process."

  def setPagesTag_Remove(self, pages, tag, applyTagFlag, session):
    """ Tag the pages with the given tag which can be a custom tag or 'Relevant'/'Irrelevant' which indicate relevance or irrelevance to the domain of interest. Tags help in clustering and categorizing the pages. They also help build computational models of the domain.

    Parameters:
        pages (urls): list of urls to apply tag
        tag (string): custom tag, 'Relevant', 'Irrelevant'
        applyTagFlag (bool): True - Add tag, False - Remove tag
        session (json): Should contain domainId

    Returns:
       Returns string "Completed Process"

    """

    es_info = self._esInfo(session['domainId'])

    entries = {}
    results = get_documents(pages, es_info['mapping']['url'], [es_info['mapping']['tag']], es_info['activeDomainIndex'], es_info['docType'],  self._es)

    if applyTagFlag and len(results) > 0:
      print '\n\napplied tag ' + tag + ' to pages' + str(pages) + '\n\n'

      for page in pages:
        if not results.get(page) is None:
          # pages to be tagged exist
          records = results[page]
          for record in records:
            entry = {}
            if record.get(es_info['mapping']['tag']) is None:
              # there are no previous tags
              entry[es_info['mapping']['tag']] = [tag]
              entry["unsure_tag"] = 0
              entry["label_pos"] = 0
              entry["label_neg"] = 0
            else:
              tags = record[es_info['mapping']['tag']]
              if len(tags) != 0:
                # previous tags exist
                if not tag in tags:
                    #Remove Relevant or Irrelevant tags if it exists
                    if tag == 'Relevant' or tag == 'Irrelevant':
                        for tag_in in tags:
                            if tag_in == 'Relevant' or tag_in == 'Irrelevant':
                                tags.remove(tag_in)
                        entry[es_info['mapping']['tag']] = tags
                    # append new tag
                    tags.append(tag)
                    entry[es_info['mapping']['tag']] = tags
                    entry["unsure_tag"] = 0
                    entry["label_pos"] = 0
                    entry["label_neg"] = 0

                    self._removeClassifierSample(session['domainId'], record['id'])

              else:
                # add new tag
                entry[es_info['mapping']['tag']] = [tag]
                entry["unsure_tag"] = 0
                entry["label_pos"] = 0
                entry["label_neg"] = 0

            if entry:
                  entries[record['id']] =  entry

    elif len(results) > 0:
      print '\n\nremoved tag ' + tag + ' from pages' + str(pages) + '\n\n'

      for page in pages:
        if not results.get(page) is None:
          records = results[page]
          for record in records:
            entry = {}
            if not record.get(es_info['mapping']['tag']) is None:
              tags = record[es_info['mapping']['tag']]
              for tag_in in tags:
                  #if tag_in == 'Relevant' or tag_in == 'Irrelevant':
                  tags.remove(tag_in)
              entry[es_info['mapping']['tag']] = tags
              entries[record['id']] = entry


    if entries:
      update_try = 0
      while (update_try < 10):
        try:
          update_document(entries, es_info['activeDomainIndex'], es_info['docType'], self._es)
          break
        except:
          update_try = update_try + 1

      if (session['domainId'] in self._onlineClassifiers) and (not applyTagFlag) and (tag in ["Relevant", "Irrelevant"]):
        self._onlineClassifiers.pop(session['domainId'])

    return "Completed Process."


  def setAllPagesTag(self, pages, tag, applyTagFlag, session):
    """ Tag ALL the pages with the given tag which can be a custom tag or 'Relevant'/'Irrelevant' which indicate relevance or irrelevance to the domain of interest. Tags help in clustering and categorizing the pages. They also help build computational models of the domain.

    Parameters:
        pages (urls): list of urls to apply tag
        tag (string): custom tag, 'Relevant', 'Irrelevant'
        applyTagFlag (bool): True - Add tag, False - Remove tag
        session (json): Should contain domainId

    Returns:
       Returns string "Completed Process"

    """
    session['pagesCap']=1000000
    """ Find pages that satisfy the specified criteria. One or more of the following criteria are specified in the session object as 'pageRetrievalCriteria':
    'Most Recent', 'More like', 'Queries', 'Tags', 'Model Tags', 'Maybe relevant', 'Maybe irrelevant', 'Unsure' and filter by keywords specified in the session object as 'filter'
    Parameters:
        session (json): Should contain 'domainId','pageRetrievalCriteria' or 'filter'
    Returns:
        json: {url1: {snippet, image_url, title, tags, retrieved}} (tags are a list, potentially empty)
    """
    es_info = self._esInfo(session['domainId'])
    #session['pagesCap'] = 12

    if session.get('from') is None:
      session['from'] = 0

    format = '%m/%d/%Y %H:%M %Z'
    if not session.get('fromDate') is None:
      session['fromDate'] = long(DomainModel.convert_to_epoch(datetime.strptime(session['fromDate'], format)))

    if not session.get('toDate') is None:
      session['toDate'] = long(DomainModel.convert_to_epoch(datetime.strptime(session['toDate'], format)))

    results = self._getPagesQuery(session)
    records = results.get('results')
    urls = []
    for rec in records:
        urls.append(rec[es_info['mapping']['url']][0])
    self.setPagesTag_Remove(urls, tag, applyTagFlag, session)

    return "Completed Process."

  def setDomainsTag(self, tlds, tag, applyTagFlag, session):
    """ Tag the pages of a domain with the given tag which can be a custom tag or 'Relevant'/'Irrelevant' which indicate relevance or irrelevance to the domain of interest. Tags help in clustering and categorizing the pages. They also help build computational models of the domain.

    Parameters:
        domains (tlds): list of domains to apply tag
        tag (string): custom tag, 'Relevant', 'Irrelevant'
        applyTagFlag (bool): True - Add tag, False - Remove tag
        session (json): Should contain domainId

    Returns:
       Returns string "Completed Process"

    """

    es_info = self._esInfo(session['domainId'])

    for tld in tlds:
      results = term_search('domain', [tld], 0, self._all, es_info['mapping']['url'], es_info['activeDomainIndex'], es_info['docType'],  self._es)

      pages = [result[es_info['mapping']['url']][0] for result in results["results"]]

      self.setPagesTag(pages, tag, applyTagFlag, session)

    return "Completed Process."

  def setTermsTag(self, terms, tag, applyTagFlag, session):
    # TODO(Yamuna): Apply tag to page and update in elastic search. Suggestion: concatenate tags
    # with semi colon, removing repetitions.
    """ Tag the terms as 'Positive'/'Negative' which indicate relevance or irrelevance to the domain of interest. Tags help in reranking terms to show the ones relevan to the domain.

    Parameters:
        terms (string): list of terms to apply tag
        tag (string): 'Positive' or 'Negative'
        applyTagFlag (bool): True - Add tag, False - Remove tag
        session (json): Should contain domainId

    Returns:
        None

    """
    es_info = self._esInfo(session['domainId'])

    ids = []
    for term in terms:
      ids.append(term+"_"+es_info['activeDomainIndex']+"_"+ es_info['docType'])

    tags = get_documents_by_id(ids, ['term', 'tag'], self._termsIndex, 'terms', self._es)

    results = {result['term'][0]: result['tag'][0] for result in tags}

    add_entries = []
    update_entries = {}

    if applyTagFlag:
      for term in terms:
        if len(results) > 0:
          if results.get(term) is None:
            entry = {
              "term" : term,
              "tag" : tag,
              "index": es_info['activeDomainIndex'],
              "doc_type": es_info['docType'],
              "_id" : term+'_'+es_info['activeDomainIndex']+'_'+es_info['docType']
            }
            add_entries.append(entry)
          else:
            old_tag = results[term]
            if tag not in old_tag:
              entry = {
                "term" : term,
                "tag" : tag,
                "index": es_info['activeDomainIndex'],
                "doc_type": es_info['docType'],
              }
              update_entries[term+'_'+es_info['activeDomainIndex']+'_'+es_info['docType']] = entry
        else:
          entry = {
            "term" : term,
            "tag" : tag,
            "index": es_info['activeDomainIndex'],
            "doc_type": es_info['docType'],
            "_id": term+'_'+es_info['activeDomainIndex']+'_'+es_info['docType']
          }
          add_entries.append(entry)
    else:
      for term in terms:
        if len(results) > 0:
          if results.get(term) is not None:
            if tag in results[term]:
              entry = {
                "term" : term,
                "tag" : "",
                "index": es_info['activeDomainIndex'],
                "doc_type": es_info['docType']
              }
              update_entries[term+'_'+es_info['activeDomainIndex']+'_'+es_info['docType']] = entry

    if add_entries:
      add_document(add_entries, self._termsIndex, 'terms', self._es)

    if update_entries:
      update_document(update_entries, self._termsIndex, 'terms', self._es)

#######################################################################################################
# Summarize Content
#######################################################################################################

  def extractTerms(self, opt_maxNumberOfTerms = 40, session = None):
    """ Extract most relevant unigrams, bigrams and trigrams that summarize the pages.
    These could provide unknown information about the domain. This in turn could
    suggest further queries for searching content.

    Parameters:
        opt_maxNumberOfTerms (int): Number of terms to return

        session (json): should have domainId

    Returns:
        array: [[term, frequencyInRelevantPages, frequencyInIrrelevantPages, tags], ...]

    """

    start_func = time.time()

    es_info = self._esInfo(session['domainId'])

    format = '%m/%d/%Y %H:%M %Z'
    if not session['fromDate'] is None:
      session['fromDate'] = long(DomainModel.convert_to_epoch(datetime.strptime(session['fromDate'], format)) * 1000)
    if not session['toDate'] is None:
      session['toDate'] = long(DomainModel.convert_to_epoch(datetime.strptime(session['toDate'], format)) * 1000)

    s_fields = {
      "tag": "Positive",
      "index": es_info['activeDomainIndex'],
      "doc_type": es_info['docType']
    }

    start = time.time()

    results = multifield_term_search(s_fields, 0, self._capTerms, ['term'], self._termsIndex, 'terms', self._es)
    pos_terms = [field['term'][0] for field in results["results"]]

    s_fields["tag"]="Negative"
    results = multifield_term_search(s_fields, 0, self._capTerms, ['term'], self._termsIndex, 'terms', self._es)
    neg_terms = [field['term'][0] for field in results["results"]]

    end = time.time()
    print "\nTime to get pos and neg terms = ", end-start,"\n"

    start = time.time()

    # Get selected pages displayed in the MDS window
    session["from"] = 0
    session["pagesCap"] = MAX_SAMPLE_PAGES

    hits = self._getPagesQuery(session)

    results = hits["results"]

    #avg_score = fsum([result["score"] for result in results])/len(results)
    avg_score = -1
    if len(results) > 0:
      avg_score = (results[0]["score"] if results[0].get("score") is not None else -1)/2.0

    end = time.time()
    print "\nTime to get query pages = ", end-start,"\n"

    top_terms = []

    start = time.time()

    text = []
    if avg_score < 0:
      #No scores available
      url_ids = [result["id"] for result in results[0:100]]
    else:
      url_ids = [result["id"] for result in results if result.get("score") > avg_score][0:100]

    urls = []
    if(len(url_ids) > 0):
      results = get_documents_by_id(url_ids, [es_info['mapping']['url'], es_info['mapping']["text"]], es_info["activeDomainIndex"], es_info["docType"], self._es)
      results = {hit['id']: " ".join(hit[es_info['mapping']["text"]][0].split(" ")[0:MAX_TEXT_LENGTH]) for hit in results if hit.get(es_info['mapping']["text"]) is not None  and hit[es_info['mapping']["text"]][0] != ""}

      urls = results.keys()
      text = results.values()

      end = time.time()
      print "\nTime to get text for 100 query pages = ", end-start,"\n"

    if len(urls) > 0 and text:
      start = time.time()

      tfidf_v = tfidf_vectorizer(max_features=1000, ngram_range=(1,3))

      [tfidf_array,_, corpus] = tfidf_v.tfidf(text)

      # tfidf_v = TfidfVectorizer(max_features=1000, stop_words="english", ngram_range=(1,3))
      # tfidf_array= tfidf_v.fit_transform(text)
      # corpus = tfidf_v.get_feature_names()

      end = time.time()
      print "\nTime to vectorize 100 query pages = ", end-start,"\n"

      if len(pos_terms) > 5:
        start = time.time()
        extract_terms_all = extract_terms.extract_terms(urls, tfidf_array, corpus)
        [ranked_terms, scores] = extract_terms_all.results(pos_terms)

        top_terms = [ term for term in ranked_terms if (term not in neg_terms and term not in pos_terms)]
        top_terms = top_terms[0:opt_maxNumberOfTerms]
        end = time.time()
        print "\nTime to rank top terms by Bayesian sets = ", end-start,"\n"
      else:
        top_terms = [term for term in tfidf_vectorizer.getTopTerms(tfidf_array, corpus, len(text), opt_maxNumberOfTerms+len(neg_terms)) if (term not in neg_terms and term not in pos_terms)]
        end = time.time()
        print "\nTime to get top terms by sorting tfidf= ", end-start,"\n"

    start = time.time()

    s_fields = {
      "tag": "Custom",
      "index": es_info['activeDomainIndex'],
      "doc_type": es_info['docType']
    }

    hits= multifield_query_search(s_fields, 0, 500, ['term'], self._termsIndex, 'terms', self._es)
    results = hits["results"]
    custom_terms = [field['term'][0] for field in results]

    for term in custom_terms:
      try:
        top_terms = top_terms.remove(term)
      except ValueError:
        continue

    if not top_terms:
      return []

    end = time.time()
    print "\nTime to remove already annotated terms = ", end-start,"\n"

    start_bar = time.time()
    start = time.time()

    hits = term_search(es_info['mapping']['tag'], ['Relevant'], 0, self._all, [es_info['mapping']['url'], es_info['mapping']['text']], es_info['activeDomainIndex'], es_info['docType'], self._es)
    results = hits["results"]
    pos_data = {field['id']:" ".join(field[es_info['mapping']['text']][0].split(" ")[0:MAX_TEXT_LENGTH]) for field in results}
    pos_urls = pos_data.keys();
    pos_text = pos_data.values();

    total_pos_tf = None
    total_pos = 0

    pos_corpus = []

    if len(pos_urls) > 1:
      tf_pos = tf_vectorizer(max_features=1000, ngram_range=(1,3))
      pos_urls_v = pos_urls
      total_pos_tf = None
      pos_corpus = None
      if self.extractTermsVectorizer.get(es_info['activeDomainIndex']) is not None:
        tf_pos = self.extractTermsVectorizer[es_info['activeDomainIndex']]["pos"]
        pos_urls_v = list(set(pos_urls).difference(set(self.extractTermsVectorizer[es_info['activeDomainIndex']]["pos_urls"])))
        if len(pos_urls_v) > 0:
          self.extractTermsVectorizer[es_info['activeDomainIndex']]["pos_urls"] = pos_urls
          pos_text = [pos_data[url] for url in pos_urls_v]
          [total_pos_tf, pos_corpus] = tf_pos.tf(pos_text)
          self.extractTermsVectorizer[es_info['activeDomainIndex']]["total_pos_tf"] = self.extractTermsVectorizer[es_info['activeDomainIndex']]["total_pos_tf"].vstack(total_pos_tf)
        else:
          if self.extractTermsVectorizer[es_info['activeDomainIndex']].get("total_pos_tf") is not None:
            total_pos_tf = self.extractTermsVectorizer[es_info['activeDomainIndex']]["total_pos_tf"]
            pos_corpus = self.extractTermsVectorizer[es_info['activeDomainIndex']]["pos_corpus"]
          else:
            pos_corpus= []
      else:
        self.extractTermsVectorizer[es_info['activeDomainIndex']] = {"pos":tf_pos, "pos_urls":pos_urls}
        [total_pos_tf, pos_corpus] = tf_pos.tf(pos_text)
        self.extractTermsVectorizer[es_info['activeDomainIndex']]["total_pos_tf"] = total_pos_tf
        self.extractTermsVectorizer[es_info['activeDomainIndex']]["pos_corpus"] = pos_corpus

      if total_pos_tf is not None:
        total_pos_tf = np.sum(total_pos_tf, axis=0)
        total_pos = np.sum(total_pos_tf)
        total_pos_tf = total_pos_tf.flatten().tolist()[0]

    end = time.time()
    print "\n Time to vectorize pos terms = ", end-start, "\n"

    start = time.time()

    hits = term_search(es_info['mapping']['tag'], ['Irrelevant'], 0, self._all, [es_info['mapping']['url'], es_info['mapping']['text']], es_info['activeDomainIndex'], es_info['docType'], self._es)
    results = hits["results"]
    neg_data = {field['id']:" ".join(field[es_info['mapping']['text']][0].split(" ")[0:MAX_TEXT_LENGTH]) for field in results}
    neg_urls = neg_data.keys();
    neg_text = neg_data.values();

    total_neg_tf = None
    total_neg = 0

    neg_corpus = []

    if len(neg_urls) > 1:
      tf_neg = tf_vectorizer(max_features=1000, ngram_range=(1,3))
      neg_urls_v = neg_urls
      total_neg_tf = None
      neg_corpus = None
      if self.extractTermsVectorizer.get(es_info['activeDomainIndex']) is not None:
        if self.extractTermsVectorizer[es_info['activeDomainIndex']].get("neg") is not None:
          tf_neg = self.extractTermsVectorizer[es_info['activeDomainIndex']]["neg"]
          neg_urls_v = list(set(neg_urls).difference(set(self.extractTermsVectorizer[es_info['activeDomainIndex']]["neg_urls"])))
          if len(neg_urls_v) > 0:
            self.extractTermsVectorizer[es_info['activeDomainIndex']]["neg_urls"] = neg_urls
            neg_text = [neg_data[url] for url in neg_urls_v]
            [total_neg_tf, neg_corpus] = tf_neg.tf(neg_text)
            self.extractTermsVectorizer[es_info['activeDomainIndex']]["total_neg_tf"] = self.extractTermsVectorizer[es_info['activeDomainIndex']]["total_neg_tf"].vstack(total_neg_tf)
          else:
            if self.extractTermsVectorizer[es_info['activeDomainIndex']].get("total_neg_tf") is not None:
              total_neg_tf = self.extractTermsVectorizer[es_info['activeDomainIndex']]["total_neg_tf"]
              neg_corpus = self.extractTermsVectorizer[es_info['activeDomainIndex']]["neg_corpus"]
            else:
              neg_corpus = []
        else:
          self.extractTermsVectorizer[es_info['activeDomainIndex']].update({"neg":tf_neg, "neg_urls":neg_urls})
          [total_neg_tf, neg_corpus] = tf_neg.tf(neg_text)
          self.extractTermsVectorizer[es_info['activeDomainIndex']]["total_neg_tf"] = total_neg_tf
          self.extractTermsVectorizer[es_info['activeDomainIndex']]["neg_corpus"] = neg_corpus

      if total_neg_tf is not None:
        total_neg_tf = np.sum(total_neg_tf, axis=0)
        total_neg = np.sum(total_neg_tf)
        total_neg_tf = total_neg_tf.flatten().tolist()[0]

    end = time.time()
    print "\n Time to vectorize neg terms = ", end-start, "\n"

    entry = {}

    start = time.time()

    for key in top_terms + pos_terms + neg_terms + [term for term in custom_terms if term in pos_corpus or term in neg_corpus]:
      entry[key] = {"pos_freq":0, "neg_freq":0, "tag":[]}
      if key in pos_corpus:
        if total_pos != 0:
          entry[key]["pos_freq"] =  (float(total_pos_tf[pos_corpus.index(key)])/total_pos)

      if key in neg_corpus:
        if total_neg != 0:
          entry[key]["neg_freq"] = (float(total_neg_tf[neg_corpus.index(key)])/total_neg)

      if key in pos_terms:
        entry[key]["tag"].append("Positive")
      elif key in neg_terms:
        entry[key]["tag"].append("Negative")

    for key in custom_terms:
      if entry.get(key) == None:
        entry[key] = {"pos_freq":0, "neg_freq":0, "tag":["Custom"]}
        if key in pos_terms:
          entry[key]["tag"].append("Positive")
        elif key in neg_terms:
          entry[key]["tag"].append("Negative")
      else:
        entry[key]["tag"].append("Custom")

    end = time.time()
    print "\n Time to compte percentage in pos and neg urls = ", end-start, "\n"

    end = time.time()
    print "\nTime to compute terms in pos and neg pages = ", end-start_bar,"\n"

    terms = [[key, entry[key]["pos_freq"], entry[key]["neg_freq"], entry[key]["tag"]] for key in custom_terms + [term for term in pos_terms if term not in custom_terms] + [term for term in neg_terms if term not in custom_terms] + top_terms ] # + top_bigrams + top_trigrams

    return terms

  def make_topic_model(self, session, tokenizer, vectorizer, model, ntopics):
    """Build topic model from the corpus of the supplied DDT domain.

    The topic model is represented as a topik.TopikProject object, and is
    persisted in disk, recording the model parameters and the location of the
    data. The output of the topic model itself is stored in Elasticsearch.

    Parameters:

        domain (str): DDT domain name as stored in Elasticsearch, so lowercase and with underscores in place of spaces.

        tokenizer (str): A tokenizer from ``topik.tokenizer.registered_tokenizers``

        vectorizer (str): A vectorization method from ``topik.vectorizers.registered_vectorizers``

        model (str): A topic model from ``topik.vectorizers.registered_models``

        ntopics (int): The number of topics to be used when modeling the corpus.

    Returns:

        model: topik model, encoding things like term frequencies, etc.
    """
    es_info = self._esInfo(session['domainId'])
    content_field = self._mapping['text']

    def not_empty(doc): return bool(doc[content_field][0])  # True if document not empty

    raw_data = filter(not_empty, get_all_ids(fields=['text'], es_index=es_info['activeDomainIndex'], es = self._es))

    id_doc_pairs = ((hash(__[content_field][0]), __[content_field][0]) for __ in raw_data)

    def not_empty_tokens(toks): return bool(toks[1])  # True if document not empty

    tokens = filter(not_empty_tokens, tokenize(id_doc_pairs, method=tokenizer))

    vectors = vectorize(tokens, method=vectorizer)
    model = run_model(vectors, model_name=model, ntopics=ntopics)

    return {"model": model, "domain": es_info['activeDomainIndex']}


#######################################################################################################
# Organize Content
#######################################################################################################

  def getPagesProjection(self, session):
    """ Organize content by some criteria such as relevance, similarity or category which allows to easily analyze groups of pages. The 'x','y' co-ordinates returned project the page in 2D maintaining clustering based on the projection chosen. The projection criteria is specified in the session object

    Parameters:
        session: Should Contain 'domainId' \
                 Should contain 'activeProjectionAlg' which takes values 'tsne', 'pca' or 'kmeans' currently

    Returns dictionary in the format:{ \
      'last_downloaded_url_epoch': 1432310403 (in seconds) \
      'pages': [ \
                [url1, x, y, tags, retrieved],     (tags are a list, potentially empty) \
                [url2, x, y, tags, retrieved], \
                [url3, x, y, tags, retrieved],
      ]\
    }
    """
    es_info = self._esInfo(session['domainId'])

    format = '%m/%d/%Y %H:%M %Z'
    if not session['fromDate'] is None:
      session['fromDate'] = long(DomainModel.convert_to_epoch(datetime.strptime(session['fromDate'], format)))

    if not session['toDate'] is None:
      session['toDate'] = long(DomainModel.convert_to_epoch(datetime.strptime(session['toDate'], format)))

    hits = self._getPagesQuery(session)

    return self._generatePagesProjection(hits, session)

  def _generatePagesProjection(self, hits, session):
    es_info = self._esInfo(session['domainId'])

    last_downloaded_url_epoch = None
    docs = []

    for i, hit in enumerate(hits):
      if last_downloaded_url_epoch is None:
        if not hit.get(es_info['mapping']['timestamp']) is None:
          last_downloaded_url_epoch = str(hit[es_info['mapping']['timestamp']][0])

      doc = ["", 0, 0, [], "", ""]

      if not hit.get(es_info['mapping']['url']) is None:
        doc[0] = hit.get(es_info['mapping']['url'])
      if not hit.get('x') is None:
        doc[1] = hit['x'][0]
      if not hit.get('y') is None:
        doc[2] = hit['y'][0]
      if not hit.get(es_info['mapping']['tag']) is None:
        doc[3] = hit[es_info['mapping']['tag']]
      if not hit.get('id') is None:
        doc[4] = hit['id']
      if not hit.get(es_info['mapping']["text"]) is None:
        doc[5] = " ".join(hit[es_info['mapping']["text"]][0].split(" ")[0:MAX_TEXT_LENGTH])

      if doc[5] != "":
        docs.append(doc)

    if len(docs) > 1:
      # Prepares results: computes projection.
      # Update x, y for pages after projection is done.

      projectionData = self.projectPages(docs, session['activeProjectionAlg'], es_info=es_info)

      last_download_epoch = last_downloaded_url_epoch
      try:
        format = '%Y-%m-%dT%H:%M:%S.%f'
        if '+' in last_downloaded_url_epoch:
          format = '%Y-%m-%dT%H:%M:%S+0000'
        last_download_epoch = DomainModel.convert_to_epoch(datetime.strptime(last_downloaded_url_epoch, format))
      except ValueError:
        try:
          format = '%Y-%m-%d %H:%M:%S.%f'
          last_download_epoch = DomainModel.convert_to_epoch(datetime.strptime(last_downloaded_url_epoch, format))
        except ValueError:
          pass

      return {\
              'last_downloaded_url_epoch':  last_download_epoch,
              'pages': projectionData
            }
    elif len(docs) == 1:
      doc = docs[0]
      return {'pages': [[doc[0],1,1,doc[3]]]}
    else:
      return {'pages': []}

#######################################################################################################
# Filter Content
#######################################################################################################

  def getPages(self, session):
    """ Find pages that satisfy the specified criteria. One or more of the following criteria are specified
    in the session object as 'pageRetrievalCriteria':

    'Most Recent', 'More like', 'Queries', 'Tags', 'Model Tags', 'Maybe relevant', 'Maybe irrelevant', 'Unsure'

    and filter by keywords specified in the session object as 'filter'

    Parameters:
        session (json): Should contain 'domainId','pageRetrievalCriteria' or 'filter'

    Returns:
        json: {url1: {snippet, image_url, title, tags, retrieved}} (tags are a list, potentially empty)

    """
    es_info = self._esInfo(session['domainId'])

    #session['pagesCap'] = 12

    if session.get('from') is None:
      session['from'] = 0

    format = '%m/%d/%Y %H:%M %Z'
    if not session.get('fromDate') is None:
      session['fromDate'] = long(DomainModel.convert_to_epoch(datetime.strptime(session['fromDate'], format)))

    if not session.get('toDate') is None:
      session['toDate'] = long(DomainModel.convert_to_epoch(datetime.strptime(session['toDate'], format)))

    results = self._getPagesQuery(session)

    hits = results.get('results')

    if hits is None:
      return {'total': 0, 'results': {}}

    no_image_desc_ids = Set()
    docs = {}
    order = 0
    for hit in hits:
      doc = {}
      if not hit.get(es_info['mapping']['description']) is None:
        doc["snippet"] = " ".join(hit[es_info['mapping']['description']][0].split(" ")[0:20])
      else:
        no_image_desc_ids.add(hit["id"])
      if not hit.get('image_url') is None:
        doc["image_url"] = hit['image_url'][0]
      else:
        no_image_desc_ids.add(hit["id"])
      if not hit.get('title') is None:
        doc["title"] = hit['title'][0]
      if not hit.get(es_info['mapping']['tag']) is None:
        doc["tags"] = hit[es_info['mapping']['tag']]
      if not hit.get("rank") is None:
        doc["rank"] = hit["rank"]
      if not hit.get(es_info['mapping']["timestamp"]) is None:
        doc["timestamp"] = hit[es_info['mapping']["timestamp"]][0]

      # To maintain order on the client
      doc["order"] = order
      order = order + 1

      docs[hit['id']] = doc

    if len(no_image_desc_ids) > 0:

      image_desc_hits = get_documents_by_id(list(no_image_desc_ids), ["url", "html", "text"], es_info['activeDomainIndex'], es_info['docType'], self._es)

      for image_desc_hit in image_desc_hits:
        if image_desc_hit.get('html') is not None:
          imageURL = getImage(image_desc_hit["html"][0], image_desc_hit['url'][0])

          if imageURL is not None:
            docs[image_desc_hit['url'][0]]['image_url'] = imageURL

          text = image_desc_hit['text'][0] if image_desc_hit.get('text') is not None else ""
          desc = getDescription(image_desc_hit["html"][0], text)
          if desc is not None:
            docs[image_desc_hit['url'][0]]['snippet'] =  " ".join(desc.split(" ")[0:20])

    return {'total': results['total'], 'results': docs}

  def _getMostRecentPages(self, session, fields_radviz=[]):

    es_info = self._esInfo(session['domainId'])
    fields = []
    if not fields_radviz:
        fields = [es_info['mapping']['url'], es_info['mapping']["description"], "image_url", "title", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"], es_info['mapping']["text"]]
    else:
        fields =fields_radviz
    results = []

    if session['fromDate'] is None and session['filter'] is None:
      results = get_most_recent_documents(session['from'], session['pagesCap'],
                                       es_info['mapping'],
                                       fields,
                                       session['filter'],
                                       es_info['activeDomainIndex'],
                                       es_info['docType'],
                                       self._es)
    elif not session['fromDate'] is None and session['filter'] is None:
        results = range_search(es_info['mapping']["timestamp"], session['fromDate'], session['toDate'],
                            fields,
                            True, session['from'], session['pagesCap'],
                            es_info['activeDomainIndex'],
                            es_info['docType'],
                            self._es)
    elif not session['filter'] is None:
        s_fields = {}
        if not session['fromDate'] is None:
          es_info['mapping']["timestamp"] = "[" + str(session['fromDate']) + " TO " + str(session['toDate']) + "]"
          fields[7] = es_info['mapping']["timestamp"]

        or_terms = session['filter'].split('OR')
        match_queries = []
        text_fields = [es_info['mapping']["text"], es_info['mapping']["title"]+"^2"]
        if not es_info['mapping'].get("domain") is None:
          text_fields.append(es_info['mapping']["domain"]+"^3")
        for term in or_terms:
          match_queries.append([term, text_fields])
        s_fields['multi_match'] = match_queries
        results = multifield_term_search(s_fields,
                                         session['from'], session['pagesCap'],
                                         fields,
                                         es_info['activeDomainIndex'],
                                         es_info['docType'],
                                         self._es)

        sorted_results = sorted(results["results"], key=lambda result: result["score"], reverse=True)
        results["results"] = sorted_results

    return results

  def _getPagesForMultiCriteria(self, session, fields_radviz=[]):
    es_info = self._esInfo(session['domainId'])
    fields = []
    if not fields_radviz:
        fields = [es_info['mapping']['url'], es_info['mapping']['description'], "image_url", "title", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"], es_info['mapping']["text"]]
    else:
        fields=fields_radviz

    s_fields = {}
    query = None
    if not session['filter'] is None:
      query = "(" + es_info['mapping']["text"] + ":" + session['filter'].replace('"','\"') + ") OR (" + \
              es_info['mapping']["title"] + ":" + session['filter'].replace('"','\"') + ") OR (" + \
              es_info['mapping']["domain"] + ":" + session['filter'].replace('"','\"') + ")"

    if not session['fromDate'] is None:
      s_fields[es_info['mapping']["timestamp"]] = "[" + str(session['fromDate']) + " TO " + str(session['toDate']) + "]"

    queries = []
    for field, value in s_fields.items():
      if query is None:
        query = "(" + field + ":" + value + ")"
      else:
        query = query + " AND " + "(" + field + ":" + value + ")"


    n_criteria = session['pageRetrievalCriteria'].keys()
    n_criteria_vals = [val.split(",") for val in session['pageRetrievalCriteria'].values()]

    criteria_comb = product(*[range(0,len(val)) for val in n_criteria_vals])

    for criteria in criteria_comb:
      i = 0
      filters = []
      for criterion_index in criteria:
        criterion = n_criteria_vals[i][criterion_index]
        n_criterion = n_criteria[i]
        if n_criterion == es_info['mapping']["tag"] and criterion == "Neutral":
          filters.append({"missing" : { "field" : es_info['mapping']["tag"] }})
        elif n_criterion == es_info['mapping']["query"] and "Crawled Data" in criterion:
          filters.append({"missing" : { "field" : es_info['mapping']["query"] }})
        elif n_criterion == 'model_tag':
          model_tags = self._onlineClassifiers[session['domainId']].get('model_tags')
          if not model_tags is None:
            model_tag_filters = []
            for url, tag in model_tags.items():
              if tag in criterion:
                model_tag_filters.append({"term":{es_info['mapping']['url']:url}})
            filters.append({"or":model_tag_filters})
        elif n_criterion == 'crawled_tag':
          if criterion == "CD Relevant":
            filters.append({"term":{"isRelevant":"relevant"}})
          elif criterion == "CD Irrelevant":
            filters.append({"term":{"isRelevant":"irrelevant"}})
        else:
          filters.append({"term":{n_criterion: criterion}})
        i = i+1

      filtered = {}
      if not query is None:
        filtered["query"]={"query_string":{"query": query}}
      if len(filters) > 0:
        filtered["filter"] = {"and":filters}

      if len(filtered) > 0:
        queries.append({"filtered":filtered})

    query = {}
    if len(queries) > 0:
      query["query"] =  {
        "bool": {
          "should": queries,
          "minimum_number_should_match": 1
        }
      }

    results = exec_query(query,
                         fields,
                         session['from'], session['pagesCap'],
                         es_info['activeDomainIndex'],
                         es_info['docType'],
                         self._es)

    sorted_results = sorted(results["results"], key=lambda result: result["score"], reverse=True)
    results["results"] = sorted_results

    # if session['selected_morelike']=="moreLike":
    #   morelike_result = self._getMoreLikePagesAll(session, results['results'])
    #   hits['results'].extend(morelike_result)
    # else:
    #   hits.extend(results)

    return results

  # def _use_rank(record):
  #   return (result.get("rank") is  None)? result["score"]: result.get("rank")

  def _getPagesForQueries(self, session, fields_radviz=[]):
    es_info = self._esInfo(session['domainId'])
    fields = []
    if not fields_radviz:
        fields = [es_info['mapping']['url'], es_info['mapping']['description'], "image_url", "title", "rank", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"]]
    else:
        fields=fields_radviz

    sorting_criteria = "rank"

    s_fields = {}
    if not session['filter'] is None:
      or_terms = session['filter'].split('OR')
      match_queries = []
      text_fields = [es_info['mapping']["text"], es_info['mapping']["title"]+"^2"]
      if not es_info['mapping'].get("domain") is None:
        text_fields.append(es_info['mapping']["domain"]+"^3")

      for term in or_terms:
        match_queries.append([term, text_fields])
      
      s_fields['multi_match'] = match_queries

      sorting_criteria = "score"
    else:
      s_fields["sort"] = [{
        "rank": {
          "order": "asc",
          "ignore_unmapped" : True
        }
      }]

    if not session['fromDate'] is None:
      s_fields[es_info['mapping']["timestamp"]] = "[" + str(session['fromDate']) + " TO " + str(session['toDate']) + "]"

    queries = session['selected_queries'].split(',')

    filters = []
    for query in queries:
      filters.append({"term":{es_info["mapping"]["query"]:query}})

    if len(filters) > 0:
      s_fields["filter"] = {"or":filters}

    results= multifield_term_search(s_fields,
                                    session['from'],
                                    session['pagesCap'],
                                    fields,
                                    es_info['activeDomainIndex'],
                                    es_info['docType'],
                                    self._es)

    if sorting_criteria == "rank":
      sorted_results = sorted(results["results"], key=lambda result: result["score"] if result.get("rank") is None else int(result["rank"][0]))
    elif sorting_criteria == "score":
      sorted_results = sorted(results["results"], key=lambda result: result["score"], reverse=True)

    results["results"] = sorted_results

    #TODO: Revisit when allowing selected_morelike
    # if session['selected_morelike']=="moreLike":
    #   aux_result = self._getMoreLikePagesAll(session, results)
    #   hits.extend(aux_result)
    # else:
    #   hits.extend(results)

    return results

  def _getPagesForTLDs(self, session, fields_radviz=[]):
    es_info = self._esInfo(session['domainId'])
    fields = []
    if not fields_radviz:
        fields = [es_info['mapping']['url'], es_info['mapping']['description'], "image_url", "title", "rank", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"]]
    else:
        fields = fields_radviz

    s_fields = {}
    if not session['filter'] is None:
      or_terms = session['filter'].split('OR')
      match_queries = []
      text_fields = [es_info['mapping']["text"], es_info['mapping']["title"]+"^2"]
      if not es_info['mapping'].get("domain") is None:
        text_fields.append(es_info['mapping']["domain"]+"^3")
      
      for term in or_terms:
        match_queries.append([term, text_fields])
      s_fields['multi_match'] = match_queries


    if not session['fromDate'] is None:
      s_fields[es_info['mapping']["timestamp"]] = "[" + str(session['fromDate']) + " TO " + str(session['toDate']) + "]"

    filters=[]
    tlds = session['selected_tlds'].split(',')

    for tld in tlds:
      filters.append({"term": {es_info['mapping']['domain']:tld}})

    if len(filters) > 0:
      s_fields["filter"] = {"or":filters}

    results= multifield_term_search(s_fields,
                                       session['from'],
                                       session['pagesCap'],
                                       fields,
                                       es_info['activeDomainIndex'],
                                       es_info['docType'],
                                       self._es)

    sorted_results = sorted(results["results"], key=lambda result: result["score"], reverse=True)
    results["results"] = sorted_results

      # if session['selected_morelike']=="moreLike":
      #   aux_result = self._getMoreLikePagesAll(session, results)
      #   hits.extend(aux_result)
      # else:
      #   hits.extend(results)

    return results

  def _getPagesForTags(self, session, fields_radviz=[]):
    es_info = self._esInfo(session['domainId'])
    fields = []
    if not fields_radviz:
        fields = [es_info['mapping']['url'], es_info['mapping']['description'], "image_url", "title", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"], es_info['mapping']["text"]]
    else:
        fields = fields_radviz

    s_fields = {}
    if not session['filter'] is None:
      or_terms = session['filter'].split('OR')
      match_queries = []
      text_fields = [es_info['mapping']["text"], es_info['mapping']["title"]+"^2"]
      if not es_info['mapping'].get("domain") is None:
        text_fields.append(es_info['mapping']["domain"]+"^3")
      
      for term in or_terms:
        match_queries.append([term, text_fields])
      s_fields['multi_match'] = match_queries


    if not session['fromDate'] is None:
      s_fields[es_info['mapping']["timestamp"]] = "[" + str(session['fromDate']) + " TO " + str(session['toDate']) + "]"

    filters=[]
    tags = session['selected_tags'].split(',')

    for tag in tags:
      if tag != "":
        if tag == "Neutral":
          filters.append({"missing" : { "field" : es_info["mapping"]["tag"]}})
          filters.append({"term":{es_info["mapping"]["tag"]:""}})
        else:
          filters.append({"term":{es_info["mapping"]["tag"]:tag}})

    #filters.append({"wildcard": {es_info['mapping']["tag"]:"*" + tag + "*"}})

    if len(filters) > 0:
      s_fields["filter"] = {"or":filters}

    results = multifield_term_search(s_fields, session['from'], session['pagesCap'],
                                     fields,
                                     es_info['activeDomainIndex'],
                                     es_info['docType'],
                                     self._es)

    sorted_results = sorted(results["results"], key=lambda result: result["score"], reverse=True)
    results["results"] = sorted_results

    return results

  def _getPagesForCrawledTags(self, session, fields_radviz=[]):
    es_info = self._esInfo(session['domainId'])
    fields = []
    if not fields_radviz:
        fields = [es_info['mapping']['url'], es_info['mapping']['description'], "image_url", "title", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"], es_info['mapping']["text"]]
    else:
        fields = fields_radviz

    s_fields = {}
    if not session['filter'] is None:
      or_terms = session['filter'].split('OR')
      match_queries = []
      text_fields = [es_info['mapping']["text"], es_info['mapping']["title"]+"^2"]
      if not es_info['mapping'].get("domain") is None:
        text_fields.append(es_info['mapping']["domain"]+"^3")
      
      for term in or_terms:
        match_queries.append([term, text_fields])
      s_fields['multi_match'] = match_queries


    if not session['fromDate'] is None:
      s_fields[es_info['mapping']["timestamp"]] = "[" + str(session['fromDate']) + " TO " + str(session['toDate']) + "]"

    filters=[]
    tags = session['selected_crawled_tags'].split(',')

    for tag in tags:
      if tag == "CD Relevant":
        filters.append({"term":{"isRelevant":"relevant"}})
      elif tag == "CD Irrelevant":
        filters.append({"term":{"isRelevant":"irrelevant"}})

    if len(filters) > 0:
      s_fields["filter"] = {"or":filters}

    results = multifield_term_search(s_fields, session['from'], session['pagesCap'],
                                     fields,
                                     es_info['activeDomainIndex'],
                                     es_info['docType'],
                                     self._es)

    sorted_results = sorted(results["results"], key=lambda result: result["score"], reverse=True)
    results["results"] = sorted_results

    return results

  def _getPagesForModelTags(self, session, fields_radviz=[]):
    es_info = self._esInfo(session['domainId'])
    fields = []
    if not fields_radviz:
        fields = [es_info['mapping']['url'], es_info['mapping']['description'], "image_url", "title", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"], es_info['mapping']["text"]]
    else:
        fields = fields_radviz

    model_tags = self._onlineClassifiers[session['domainId']].get('model_tags')
    if model_tags is None:
      return {'total':0, 'results':[]}

    s_fields = {}
    if not session['filter'] is None:
      or_terms = session['filter'].split('OR')
      match_queries = []
      text_fields = [es_info['mapping']["text"], es_info['mapping']["title"]+"^2"]
      if not es_info['mapping'].get("domain") is None:
        text_fields.append(es_info['mapping']["domain"]+"^3")
      
      for term in or_terms:
        match_queries.append([term, text_fields])
      s_fields['multi_match'] = match_queries


    filters=[]
    tags = session['selected_model_tags'].split(',')

    for url, tag in model_tags.items():
      if tag in tags:
        filters.append({"term":{es_info['mapping']['url']:url}})

    if len(filters) > 0:
      s_fields["filter"] = {"or":filters}

    results = multifield_term_search(s_fields, session['from'], session['pagesCap'],
                                     fields,
                                     es_info['activeDomainIndex'],
                                     es_info['docType'],
                                     self._es)

    sorted_results = sorted(results["results"], key=lambda result: result["score"], reverse=True)
    results["results"] = sorted_results

    return results

  def _getRelevantPages(self, session):
    es_info = self._esInfo(session['domainId'])

    pos_hits = search(es_info['mapping']['tag'], ['relevant'], session['from'], session['pagesCap'], [es_info['mapping']['url'], es_info['mapping']['description'], "image_url", "title", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"], es_info['mapping']["text"]], es_info['activeDomainIndex'], 'page', self._es)

    return pos_hits

  def _getMoreLikePages(self, session, reverse=[], fields_radviz=[]):
    es_info = self._esInfo(session['domainId'])
    fields = []
    if not fields_radviz:
        fields = [es_info['mapping']['url'], es_info['mapping']['description'], "image_url", "title", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"], es_info['mapping']["text"]]
    else:
        fields=fields_radviz

    hits=[]
    tags = session['selected_tags'].split(',')
    for tag in tags:
      tag_hits = search(es_info['mapping']['tag'], [tag], session['from'], session['pagesCap'], fields, es_info['activeDomainIndex'], 'page', self._es)

      if len(tag_hits) > 0:
        tag_urls = [field['id'] for field in tag_hits]

        results = get_more_like_this(tag_urls, fields, session['from'], session['from'], session['pagesCap'],  es_info['activeDomainIndex'], es_info['docType'],  self._es)

        hits.extend(tag_hits[0:self._pagesCapTerms] + results)

    return hits

  def _getMoreLikePagesAll(self, session, tag_hits):
      es_info = self._esInfo(session['domainId'])
      if len(tag_hits) > 0:
          tag_urls = [field['id'] for field in tag_hits]
          results = get_more_like_this(tag_urls, [es_info['mapping']['url'], es_info['mapping']['description'], "image_url", "title", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"], es_info['mapping']["text"]], session['pagesCap'],  es_info['activeDomainIndex'], es_info['docType'],  self._es)
          aux_result = tag_hits[0:self._pagesCapTerms] + results
      else: aux_result=tag_hits
      return aux_result

  def _getPagesQuery(self, session):
    es_info = self._esInfo(session['domainId'])

    format = '%m/%d/%Y %H:%M %Z'
    if not session.get('fromDate') is None:
      session['fromDate'] = long(DomainModel.convert_to_epoch(datetime.strptime(session['fromDate'], format)))

    if not session.get('toDate') is None:
      session['toDate'] = long(DomainModel.convert_to_epoch(datetime.strptime(session['toDate'], format)))

    hits = []

    if(session.get('pageRetrievalCriteria') == 'Most Recent'):
      hits = self._getMostRecentPages(session)
    elif (session.get('pageRetrievalCriteria') == 'More like'):
      hits = self._getMoreLikePages(session)
    elif (session.get('newPageRetrievalCriteria') == 'Multi'):
       hits = self._getPagesForMultiCriteria(session)
    elif (session.get('pageRetrievalCriteria') == 'Queries'):
      hits = self._getPagesForQueries(session)
    elif (session.get('pageRetrievalCriteria') == 'TLDs'):
      hits = self._getPagesForTLDs(session)
    elif (session.get('pageRetrievalCriteria') == 'Tags'):
      hits = self._getPagesForTags(session)
    elif (session.get('pageRetrievalCriteria') == 'Model Tags'):
      hits = self._getPagesForModelTags(session)
    elif (session.get('pageRetrievalCriteria') == 'Crawled Tags'):
      hits = self._getPagesForCrawledTags(session)

    return hits

  def getTextQuery(self, session):
    es_info = self._esInfo(session['domainId'])

    format = '%m/%d/%Y %H:%M %Z'
    if not session.get('fromDate') is None:
      session['fromDate'] = long(DomainModel.convert_to_epoch(datetime.strptime(session['fromDate'], format)))

    if not session.get('toDate') is None:
      session['toDate'] = long(DomainModel.convert_to_epoch(datetime.strptime(session['toDate'], format)))

    hits = []
    fields_radviz = [es_info['mapping']['url'], es_info['mapping']["tag"], es_info['mapping']["text"], es_info['mapping']['description'], "image_url", es_info['mapping']["title"]]

    if(session.get('pageRetrievalCriteria') == 'Most Recent'):
      hits = self._getMostRecentPages(session, fields_radviz)
    elif (session.get('pageRetrievalCriteria') == 'More like'):
      hits = self._getMoreLikePages(session, fields_radviz)
    elif (session.get('newPageRetrievalCriteria') == 'Multi'):
       hits = self._getPagesForMultiCriteria(session, fields_radviz)
    elif (session.get('pageRetrievalCriteria') == 'Queries'):
      hits = self._getPagesForQueries(session, fields_radviz)
    elif (session.get('pageRetrievalCriteria') == 'TLDs'):
      hits = self._getPagesForTLDs(session, fields_radviz)
    elif (session.get('pageRetrievalCriteria') == 'Tags'):
      hits = self._getPagesForTags(session, fields_radviz)
    elif (session.get('pageRetrievalCriteria') == 'Model Tags'):
      hits = self._getPagesForModelTags(session, fields_radviz)
    elif (session.get('pageRetrievalCriteria') == 'Crawled Tags'):
      hits = self._getPagesForCrawledTags(session, fields_radviz)

    return hits


#######################################################################################################
# Generate Model
#######################################################################################################

# Update online classifier (it will only use the tags: Relevant and Irrelevant to create the online classifier)
  def updateOnlineClassifier(self, session):
    domainId = session['domainId']
    es_info = self._esInfo(domainId)

    onlineClassifier = None
    trainedPosSamples = []
    trainedNegSamples = []
    onlineClassifierInfo = self._onlineClassifiers.get(domainId)

    if onlineClassifierInfo == None:
      onlineClassifier = OnlineClassifier()
      self._onlineClassifiers[domainId] = {"onlineClassifier":onlineClassifier}
      self._onlineClassifiers[domainId]["trainedPosSamples"] = []
      self._onlineClassifiers[domainId]["trainedNegSamples"] = []
    else:
      onlineClassifier = self._onlineClassifiers[domainId]["onlineClassifier"]
      trainedPosSamples = self._onlineClassifiers[domainId]["trainedPosSamples"]
      trainedNegSamples = self._onlineClassifiers[domainId]["trainedNegSamples"]

    # Fit classifier
    # ****************************************************************************************
    query = {
      "query": {
        "bool" : {
          "must" : {
            "term" : { "tag" : "Relevant" }
          }
        }
      },
      "filter": {
        "not": {
          "filter": {
            "ids" : {
              "values" : trainedPosSamples
            }
          }
        }
      }
    }

    results = exec_query(query, [es_info['mapping']['url'], es_info['mapping']['text']],
                          0, self._all,
                          es_info['activeDomainIndex'],
                          es_info['docType'],
                          self._es)
    pos_docs = results["results"]

    pos_text = [pos_doc[es_info['mapping']['text']][0][0:MAX_TEXT_LENGTH] for pos_doc in pos_docs]
    pos_ids = [pos_doc["id"] for pos_doc in pos_docs]
    pos_labels = [1 for i in range(0, len(pos_text))]

    query = {
      "query": {
        "bool" : {
          "must" : {
            "term" : { "tag" : "Irrelevant" }
          }
        }
      },
      "filter": {
        "not": {
          "filter": {
            "ids" : {
              "values" : trainedNegSamples
            }
          }
        }
      }
    }
    results = exec_query(query, [es_info['mapping']['url'], es_info['mapping']['text']],
                          0, self._all,
                          es_info['activeDomainIndex'],
                          es_info['docType'],
                          self._es)

    neg_docs = results["results"]

    neg_text = [neg_doc[es_info['mapping']['text']][0][0:MAX_TEXT_LENGTH] for neg_doc in neg_docs]
    neg_ids = [neg_doc["id"] for neg_doc in neg_docs]
    neg_labels = [0 for i in range(0, len(neg_text))]

    print "\n\n\nNew relevant samples ", len(pos_text),"\n", "New irrelevant samples ",len(neg_text), "\n\n\n"

    clf = None
    train_data = None
    update = False
    if (len(trainedPosSamples) > 0 and len(trainedNegSamples) > 0):
      if (len(pos_text) > 0 or len(neg_text) > 0):
        update = True
    else:
      if (len(pos_text) > 0 and len(neg_text) > 0):
        update = True
    if update:
      if self._onlineClassifiers.get(domainId) is not None:
        [train_data,_] = self._onlineClassifiers[domainId]["onlineClassifier"].vectorize(pos_text+neg_text)
        clf = self._onlineClassifiers[domainId]["onlineClassifier"].partialFit(train_data, pos_labels+neg_labels)
        if clf != None:
          self._onlineClassifiers[domainId]["trainedPosSamples"] = self._onlineClassifiers[domainId]["trainedPosSamples"] + pos_ids
          self._onlineClassifiers[domainId]["trainedNegSamples"] = self._onlineClassifiers[domainId]["trainedNegSamples"] + neg_ids

    # ****************************************************************************************

    # Fit calibratrated classifier

    if train_data != None and clf != None:

      trainedPosSamples = self._onlineClassifiers[domainId]["trainedPosSamples"]
      trainedNegSamples = self._onlineClassifiers[domainId]["trainedNegSamples"]
      if 2*len(trainedPosSamples)/3  > 2 and  2*len(trainedNegSamples)/3 > 2:
        pos_trained_docs = get_documents_by_id(trainedPosSamples,
                                               [es_info['mapping']['url'], es_info['mapping']['text']],
                                               es_info['activeDomainIndex'],
                                               es_info['docType'],
                                               self._es)
        pos_trained_text = [pos_trained_doc[es_info['mapping']['text']][0][0:MAX_TEXT_LENGTH] for pos_trained_doc in pos_trained_docs]
        pos_trained_labels = [1 for i in range(0, len(pos_trained_text))]

        neg_trained_docs = get_documents_by_id(trainedNegSamples,
                                               [es_info['mapping']['url'], es_info['mapping']['text']],
                                               es_info['activeDomainIndex'],
                                               es_info['docType'],
                                               self._es)

        neg_trained_text = [neg_trained_doc[es_info['mapping']['text']][0][0:MAX_TEXT_LENGTH] for neg_trained_doc in neg_trained_docs]
        neg_trained_labels = [0 for i in range(0, len(neg_trained_text))]
        [calibrate_pos_data,_] = self._onlineClassifiers[domainId]["onlineClassifier"].vectorize(pos_trained_text)
        [calibrate_neg_data,_] = self._onlineClassifiers[domainId]["onlineClassifier"].vectorize(neg_trained_text)
        calibrate_pos_labels = pos_trained_labels
        calibrate_neg_labels = neg_trained_labels

        calibrate_data = sp.sparse.vstack([calibrate_pos_data, calibrate_neg_data]).toarray()
        calibrate_labels = calibrate_pos_labels+calibrate_neg_labels

        train_indices = np.random.choice(len(calibrate_labels), 2*len(calibrate_labels)/3)
        test_indices = np.random.choice(len(calibrate_labels), len(calibrate_labels)/3)

        sigmoid = onlineClassifier.calibrate(calibrate_data[train_indices], np.asarray(calibrate_labels)[train_indices])
        if not sigmoid is None:
          self._onlineClassifiers[domainId]["sigmoid"] = sigmoid
          accuracy = round(self._onlineClassifiers[domainId]["onlineClassifier"].calibrateScore(sigmoid, calibrate_data[test_indices], np.asarray(calibrate_labels)[test_indices]), 4) * 100
          self._onlineClassifiers[domainId]["accuracy"] = str(accuracy)

          print "\n\n\n Accuracy = ", accuracy, "%\n\n\n"
        else:
          print "\n\n\nNot enough data for calibration\n\n\n"
      else:
        print "\n\n\nNot enough data for calibration\n\n\n"

      # ****************************************************************************************


    accuracy = '0'
    if self._onlineClassifiers.get(domainId) != None:
      accuracy = self._onlineClassifiers[domainId].get("accuracy")
      if accuracy == None:
        accuracy = '0'

    # self.results_file.write(str(len(pos_text)) +","+ str(len(neg_text)) +","+ accuracy +","+ str(unsure) +","+ str(label_pos) +","+ str(label_neg) +","+ str(len(unlabeled_urls))+"\n")

    return accuracy

# Update classifier which will be used by the crawler (it will take into account the tags that were selected as positive and negative)
  def updateClassifierCrawler(self, session):
    domainId = session['domainId']
    es_info = self._esInfo(domainId)

    onlineClassifier = None
    trainedPosSamples = []
    trainedNegSamples = []
    #onlineClassifierInfo = self._classifiersCrawler.get(domainId)
    onlineClassifier = OnlineClassifier()
    self._classifiersCrawler[domainId] = {"onlineClassifier":onlineClassifier}
    self._classifiersCrawler[domainId]["trainedPosSamples"] = []
    self._classifiersCrawler[domainId]["trainedNegSamples"] = []

    filter_pos_tags = ["Relevant"]
    filter_neg_tags = ["Irrelevant"]

    try:
        filter_pos_tags = session['model']['positive']
    except KeyError:
        print "Using default positive tags"

    try:
        filter_neg_tags = session['model']['negative']
    except KeyError:
        print "Using default negative tags"

    # Fit classifier
    # ****************************************************************************************
    query = {
      "query": {
        "bool" : {
          "must" : {
            "terms" : {
                    "tag" : filter_pos_tags,
                    "minimum_should_match" : 1
                }
            }
        }
      },
      "filter": {
        "not": {
          "filter": {
            "ids" : {
              "values" : trainedPosSamples
            }
          }
        }
      }
    }

    results = exec_query(query, [es_info['mapping']['url'], es_info['mapping']['text']],
                          0, self._all,
                          es_info['activeDomainIndex'],
                          es_info['docType'],
                          self._es)
    pos_docs = results["results"]
    pos_text = [pos_doc[es_info['mapping']['text']][0][0:MAX_TEXT_LENGTH] for pos_doc in pos_docs]
    pos_ids = [pos_doc["id"] for pos_doc in pos_docs]
    pos_labels = [1 for i in range(0, len(pos_text))]

    query = {
       "query": {
         "bool" : {
           "must" : {
             "terms" : {
                     "tag" : filter_neg_tags,
                     "minimum_should_match" : 1
                 }
             }
         }
       },
      "filter": {
        "not": {
          "filter": {
            "ids" : {
              "values" : trainedNegSamples
            }
          }
        }
      }
    }
    results = exec_query(query, [es_info['mapping']['url'], es_info['mapping']['text']],
                          0, self._all,
                          es_info['activeDomainIndex'],
                          es_info['docType'],
                          self._es)

    neg_docs = results["results"]

    neg_text = [neg_doc[es_info['mapping']['text']][0][0:MAX_TEXT_LENGTH] for neg_doc in neg_docs]
    neg_ids = [neg_doc["id"] for neg_doc in neg_docs]
    neg_labels = [0 for i in range(0, len(neg_text))]

    print "\n\n\nNew relevant samples for updateClasssifierCrawler", len(pos_text),"\n", "New irrelevant samples updateClasssifierCrawler",len(neg_text), "\n\n\n"

    clf = None
    train_data = None
    update = False
    if (len(trainedPosSamples) > 0 and len(trainedNegSamples) > 0):
      if (len(pos_text) > 0 or len(neg_text) > 0):
        update = True
    else:
      if (len(pos_text) > 0 and len(neg_text) > 0):
        update = True
    if update:
      if self._classifiersCrawler.get(domainId) is not None:
        [train_data,_] = self._classifiersCrawler[domainId]["onlineClassifier"].vectorize(pos_text+neg_text)
        clf = self._classifiersCrawler[domainId]["onlineClassifier"].partialFit(train_data, pos_labels+neg_labels)
        if clf != None:
          self._classifiersCrawler[domainId]["trainedPosSamples"] = self._classifiersCrawler[domainId]["trainedPosSamples"] + pos_ids
          self._classifiersCrawler[domainId]["trainedNegSamples"] = self._classifiersCrawler[domainId]["trainedNegSamples"] + neg_ids

    # ****************************************************************************************

    # Fit calibratrated classifier

    if train_data != None and clf != None:

      trainedPosSamples = self._classifiersCrawler[domainId]["trainedPosSamples"]
      trainedNegSamples = self._classifiersCrawler[domainId]["trainedNegSamples"]
      if 2*len(trainedPosSamples)/3  > 2 and  2*len(trainedNegSamples)/3 > 2:
        pos_trained_docs = get_documents_by_id(trainedPosSamples,
                                               [es_info['mapping']['url'], es_info['mapping']['text']],
                                               es_info['activeDomainIndex'],
                                               es_info['docType'],
                                               self._es)
        pos_trained_text = [pos_trained_doc[es_info['mapping']['text']][0][0:MAX_TEXT_LENGTH] for pos_trained_doc in pos_trained_docs]
        pos_trained_labels = [1 for i in range(0, len(pos_trained_text))]

        neg_trained_docs = get_documents_by_id(trainedNegSamples,
                                               [es_info['mapping']['url'], es_info['mapping']['text']],
                                               es_info['activeDomainIndex'],
                                               es_info['docType'],
                                               self._es)

        neg_trained_text = [neg_trained_doc[es_info['mapping']['text']][0][0:MAX_TEXT_LENGTH] for neg_trained_doc in neg_trained_docs]
        neg_trained_labels = [0 for i in range(0, len(neg_trained_text))]
        [calibrate_pos_data,_] = self._classifiersCrawler[domainId]["onlineClassifier"].vectorize(pos_trained_text)
        [calibrate_neg_data,_] = self._classifiersCrawler[domainId]["onlineClassifier"].vectorize(neg_trained_text)
        calibrate_pos_labels = pos_trained_labels
        calibrate_neg_labels = neg_trained_labels

        calibrate_data = sp.sparse.vstack([calibrate_pos_data, calibrate_neg_data]).toarray()
        calibrate_labels = calibrate_pos_labels+calibrate_neg_labels

        train_indices = np.random.choice(len(calibrate_labels), 2*len(calibrate_labels)/3)
        test_indices = np.random.choice(len(calibrate_labels), len(calibrate_labels)/3)

        sigmoid = onlineClassifier.calibrate(calibrate_data[train_indices], np.asarray(calibrate_labels)[train_indices])
        if not sigmoid is None:
          self._classifiersCrawler[domainId]["sigmoid"] = sigmoid
          accuracy = round(self._classifiersCrawler[domainId]["onlineClassifier"].calibrateScore(sigmoid, calibrate_data[test_indices], np.asarray(calibrate_labels)[test_indices]), 4) * 100
          self._classifiersCrawler[domainId]["accuracy"] = str(accuracy)

          print "\n\n\n Accuracy = ", accuracy, "%\n\n\n"
        else:
          print "\n\n\nNot enough data for calibration\n\n\n"
      else:
        print "\n\n\nNot enough data for calibration\n\n\n"

      # ****************************************************************************************


    accuracy = '0'
    if self._classifiersCrawler.get(domainId) != None:
      accuracy = self._classifiersCrawler[domainId].get("accuracy")
      if accuracy == None:
        accuracy = '0'

    # self.results_file.write(str(len(pos_text)) +","+ str(len(neg_text)) +","+ accuracy +","+ str(unsure) +","+ str(label_pos) +","+ str(label_neg) +","+ str(len(unlabeled_urls))+"\n")

    return accuracy


  def _removeClassifierSample(self, domainId, sampleId):
    if self._onlineClassifiers.get(domainId) != None:
      try:
        self._onlineClassifiers[domainId]["trainedPosSamples"].remove(sampleId)
      except ValueError:
        pass
      try:
        self._onlineClassifiers[domainId]["trainedNegSamples"].remove(sampleId)
      except ValueError:
        pass


  def predictData(self, session):
    # Label unlabelled data
    #
    #TODO: Move this to Model tab functionality
    es_info = self._esInfo(session['domainId'])

    #self.updateOnlineClassifier(session)

    unsure = 0
    label_pos = 0
    label_neg = 0

    relevant_urls = []
    irrelevant_urls = []
    unsure_urls = []
    if self._onlineClassifiers.get(session['domainId']) == None:
      return

    sigmoid = self._onlineClassifiers[session['domainId']].get("sigmoid")
    if sigmoid != None:

      # Select random MAX_SAMPLE
      filters = [{ "filter" : { "missing" : { "field" : "tag"}}, "weight": 1}]
      session['pagesCap'] = 100000
      temp = self._getMostRecentPages(session)
      unlabelled_docs = temp.get('results')

      unlabelled_docs = [unlabelled_doc for unlabelled_doc in unlabelled_docs if unlabelled_doc.get(es_info['mapping']['text']) is not None]
      unlabeled_text = [unlabelled_doc[es_info['mapping']['text']][0][0:MAX_TEXT_LENGTH] for unlabelled_doc in unlabelled_docs]

      # Check if unlabeled data available
      if len(unlabeled_text) > 0:
        [unlabeled_data,_] =  self._onlineClassifiers[session['domainId']]["onlineClassifier"].vectorize(unlabeled_text)
        [classp, calibp, cm] = self._onlineClassifiers[session['domainId']]["onlineClassifier"].predictClass(unlabeled_data,sigmoid)

        pos_calib_indices = np.nonzero(calibp)
        neg_calib_indices = np.where(calibp == 0)

        pos_cm = [cm[pos_calib_indices][i][1] for i in range(0,np.shape(cm[pos_calib_indices])[0])]
        neg_cm = [cm[neg_calib_indices][i][0] for i in range(0,np.shape(cm[neg_calib_indices])[0])]

        pos_sorted_cm = pos_calib_indices[0][np.asarray(np.argsort(pos_cm)[::-1])]
        neg_sorted_cm = neg_calib_indices[0][np.asarray(np.argsort(neg_cm)[::-1])]

        model_tags = {}

        for i in pos_sorted_cm:
          if cm[i][1] < 60:
            model_tags[unlabelled_docs[i][es_info['mapping']['url']][0]] = "Unsure"
            unsure = unsure + 1
            url = unlabelled_docs[i][es_info['mapping']['url']]
            unsure_urls.append(url[0])
          else:
            model_tags[unlabelled_docs[i][es_info['mapping']['url']][0]] = "Maybe relevant"
            label_pos = label_pos + 1
            url = unlabelled_docs[i][es_info['mapping']['url']]
            relevant_urls.append(url[0])

        for i in neg_sorted_cm:
          if cm[i][0] < 60:
            model_tags[unlabelled_docs[i][es_info['mapping']['url']][0]] = "Unsure"
            unsure = unsure + 1
            url = unlabelled_docs[i][es_info['mapping']['url']]
            unsure_urls.append(url[0])
          else:
            model_tags[unlabelled_docs[i][es_info['mapping']['url']][0]] = "Maybe irrelevant"
            label_neg = label_neg + 1
            url = unlabelled_docs[i][es_info['mapping']['url']]
            irrelevant_urls.append(url[0])

    return self._crawlerModel.createResultModel(session, relevant_urls, irrelevant_urls, unsure_urls)


#######################################################################################################

  def predictUnlabeled(self, session):
    # Label unlabelled data

    #TODO: Move this to Model tab functionality
    es_info = self._esInfo(session['domainId'])

    #self.updateOnlineClassifier(session)

    unsure = 0
    label_pos = 0
    label_neg = 0

    MAX_SAMPLE = 300

    if self._onlineClassifiers.get(session['domainId']) == None:
      return

    sigmoid = self._onlineClassifiers[session['domainId']].get("sigmoid")
    if sigmoid != None:

      # Select random MAX_SAMPLE
      filters = [{ "filter" : { "missing" : { "field" : "tag"}}, "weight": 1}]
      unlabelled_docs = random_sample(None, filters,  [es_info['mapping']['url'], es_info['mapping']['text']], MAX_SAMPLE,
                                        es_info['activeDomainIndex'],
                                        es_info['docType'],
                                        self._es)



      unlabelled_docs = [unlabelled_doc for unlabelled_doc in unlabelled_docs if unlabelled_doc.get(es_info['mapping']['text']) is not None]
      unlabeled_text = [unlabelled_doc[es_info['mapping']['text']][0][0:MAX_TEXT_LENGTH] for unlabelled_doc in unlabelled_docs]

      # Check if unlabeled data available
      if len(unlabeled_text) > 0:
        [unlabeled_data,_] =  self._onlineClassifiers[session['domainId']]["onlineClassifier"].vectorize(unlabeled_text)
        [classp, calibp, cm] = self._onlineClassifiers[session['domainId']]["onlineClassifier"].predictClass(unlabeled_data,sigmoid)

        pos_calib_indices = np.nonzero(calibp)
        neg_calib_indices = np.where(calibp == 0)

        pos_cm = [cm[pos_calib_indices][i][1] for i in range(0,np.shape(cm[pos_calib_indices])[0])]
        neg_cm = [cm[neg_calib_indices][i][0] for i in range(0,np.shape(cm[neg_calib_indices])[0])]

        pos_sorted_cm = pos_calib_indices[0][np.asarray(np.argsort(pos_cm)[::-1])]
        neg_sorted_cm = neg_calib_indices[0][np.asarray(np.argsort(neg_cm)[::-1])]

        model_tags = {}
        for i in pos_sorted_cm:
          if cm[i][1] < 60:
            model_tags[unlabelled_docs[i][es_info['mapping']['url']][0]] = "Unsure"
            unsure = unsure + 1
          else:
            model_tags[unlabelled_docs[i][es_info['mapping']['url']][0]] = "Maybe relevant"
            label_pos = label_pos + 1

        for i in neg_sorted_cm:
          if cm[i][0] < 60:
            model_tags[unlabelled_docs[i][es_info['mapping']['url']][0]] = "Unsure"
            unsure = unsure + 1
          else:
            model_tags[unlabelled_docs[i][es_info['mapping']['url']][0]] = "Maybe irrelevant"
            url_t = unlabelled_docs[i][es_info['mapping']['url']]
            label_neg = label_neg + 1

        self._onlineClassifiers[session['domainId']]['model_tags'] = model_tags

    return {"Unsure": unsure, "Maybe relevant": label_pos, "Maybe irrelevant": label_neg}


#######################################################################################################

  def getPlottingData(self, session):
    es_info = self._esInfo(session['domainId'])
    return get_plotting_data(self._all, es_info["activeDomainIndex"], es_info['docType'], self._es)

  # Projects pages.
  def projectPages(self, pages, projectionType='TSNE', es_info=None):
    return self.projectionsAlg[projectionType](pages, es_info)

  # Projects pages with PCA
  def pca(self, pages, es_info=None):

    urls = [page[4] for page in pages]
    text = [page[5] for page in pages]

    [urls, data] = DomainModel.w2v.process_text(urls, text)

    pca_count = 2
    pcadata = DomainModel.runPCASKLearn(data, pca_count)

    try:
      results = []
      i = 0
      for page in pages:
        if page[4] in urls:
          pdata = [page[0], pcadata[i][0], pcadata[i][1], page[3]]
          i = i + 1
          results.append(pdata)
    except IndexError:
      print 'INDEX OUT OF BOUNDS ',i

    return results

  # Projects pages with TSNE
  def tsne(self, pages, es_info=None):

    urls = [page[4] for page in pages]
    text = [page[5] for page in pages]

    [urls, data] = DomainModel.w2v.process_text(urls, text)

    data = np.divide(data, np.sum(data, axis=0))
    tags = [page[3][0] if page[3] else "" for page in pages if page[4] in urls]
    labels = [tags.index(tag) for tag in tags]

    tsne_count = 2
    tsnedata = DomainModel.runTSNESKLearn(1-data, labels, tsne_count)

    try:
      results = []
      i = 0
      for page in pages:
        if page[4] in urls:
          pdata = [page[0], tsnedata[i][0], tsnedata[i][1], page[3]]
          i = i + 1
          results.append(pdata)

    except IndexError:
      print 'INDEX OUT OF BOUNDS ',i
    return results

  # Projects pages with KMeans
  def kmeans(self, pages):

    urls = [page[4] for page in pages]
    text = [page[5] for page in pages]

    [urls, data] = DomainModel.w2v.process_text(urls, text)

    k = 5
    kmeansdata = DomainModel.runKMeansSKLearn(data, k)

    try:
      results = []
      i = 0
      for page in pages:
        if page[4] in urls:
          pdata = [page[0], kmeansdata[1][i][0], kmeansdata[1][i][1], page[3]]
          i = i + 1
          results.append(pdata)

    except IndexError:
      print 'INDEX OUT OF BOUNDS ',i
    return results

  def term_tfidf(self, urls):
    [data, data_tf, data_ttf , corpus, urls] = getTermStatistics(urls, mapping=es_info['mapping'], es_index=es_info['activeDomainIndex'], es_doc_type=es_info['docType'], es=self._es)
    return [data, data_tf, data_ttf, corpus, urls]

  @staticmethod
  def runPCASKLearn(X, pc_count = None):
    pca = PCA(n_components=pc_count)
    return pca.fit_transform(X)

  @staticmethod
  def runTSNESKLearn(X,y=None,pc_count = None):
    tsne = TSNE(n_components=pc_count, random_state=0, metric="correlation", learning_rate=200)
    result = None
    if y != None:
      result = tsne.fit_transform(X,y)
    else:
      result = tsne.fit_transform(X)

    return result

  @staticmethod
  def runKMeansSKLearn(X, k = None):
    kmeans = KMeans(n_clusters=k, n_jobs=-1)
    clusters = kmeans.fit_predict(X).tolist()
    cluster_distance = kmeans.fit_transform(X).tolist()
    cluster_centers = kmeans.cluster_centers_

    coords = []
    for cluster in clusters:
      coord = [cluster_centers[cluster,0], cluster_centers[cluster, 1]]
      coords.append(coord)

    return [None, coords]

  @staticmethod
  def convert_to_epoch(dt):
    epoch = datetime.utcfromtimestamp(0)
    delta = dt - epoch
    return delta.total_seconds()
