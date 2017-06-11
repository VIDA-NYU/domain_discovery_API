import time
import calendar
import os
import shutil
from datetime import datetime
from dateutil import tz
from sets import Set
from itertools import product
import json;
from signal import SIGTERM
import shlex
from pprint import pprint

from sklearn.decomposition import PCA
from sklearn.manifold import TSNE
from sklearn.cluster import KMeans

import scipy as sp
import numpy as np
from random import random, randint, sample

from subprocess import Popen
from subprocess import PIPE

import linecache
from sys import exc_info
from os import chdir, listdir, environ, makedirs, rename, chmod, walk
from os.path import isfile, join, exists, isdir
from zipfile import ZipFile

from elasticsearch import Elasticsearch

from seeds_generator.download import callDownloadUrls, getImage, getDescription
from seeds_generator.runSeedFinder import RunSeedFinder
from seeds_generator.concat_nltk import get_bag_of_words
from elastic.get_config import get_available_domains, get_mapping, get_tag_colors
from elastic.search_documents import get_context, term_search, search, range_search, multifield_term_search, multifield_query_search, field_missing, field_exists, exec_query
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

    create_config_index()
    create_terms_index()

    self._mapping = {"url":"url", "timestamp":"retrieved", "text":"text", "html":"html", "tag":"tag", "query":"query", "domain":"domain", "title":"title"}
    self._domains = None
    self._onlineClassifiers = {}
    self._pos_tags = ['NN', 'NNS', 'NNP', 'NNPS', 'FW', 'JJ']
    self._path = path

    self.results_file = open("results.txt", "w")

    self.pool = Pool(max_workers=3)
    self.seedfinder = RunSeedFinder()
    self.runningCrawlers={}
    self.runningSeedFinders={}

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

  def getStatus(self, session):
    status = {}

    if len(self.runningCrawlers.keys()) > 0:
      status["crawler"] = []
      for k,v in self.runningCrawlers.items():
        status["crawler"].append({"domain": v["domain"], "status": v["status"]})

    if len(self.runningSeedFinders.keys()) > 0:
      status["seedFinder"] = []
      for k,v in self.runningSeedFinders.items():
        status["seedFinder"].append({"domain": v["domain"], "status": v["status"], "description": v["description"]})

    return status

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
    return get_unique_values('domain', self._all, es_info['activeDomainIndex'], es_info['docType'], self._es)

  def getAvailableQueries(self, session):
    """ Return all queries for the selected domain.

    Parameters:
        session (json): Should contain the domainId

    Returns:
        json: {<query>: <number of pages for the query>}

    """
    es_info = self._esInfo(session['domainId'])
    queries = get_unique_values('query', self._all, es_info['activeDomainIndex'], es_info['docType'], self._es)

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

    tags = get_unique_values('tag', self._all, es_info['activeDomainIndex'], es_info['docType'], self._es)
    for tag, num in tags.iteritems():
      if tag != "":
        if unique_tags.get(tag) is not None:
          unique_tags[tag] = unique_tags[tag] + num
        else:
          unique_tags[tag] = num
      else:
        unique_tags["Neutral"] = unique_tags["Neutral"] + 1
    return unique_tags

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
    total_results = get_most_recent_documents(2000, es_info['mapping'], ["url", es_info['mapping']["tag"]],
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
      range_search(es_info['mapping']["timestamp"], opt_ts1, opt_ts2, ['url',es_info['mapping']['tag']], True, session['pagesCap'], es_index=es_info['activeDomainIndex'], es_doc_type=es_info['docType'], es=self._es)


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
              "timestamp": datetime.utcnow(),
            }

    load_config([entry])

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
      comm = 'java -cp target/seeds_generator-1.0-SNAPSHOT-jar-with-dependencies.jar GoogleSearch -t ' + str(top) + \
             ' -q "' + terms.replace('"','\\"')  + '"' + \
             ' -i ' + es_info['activeDomainIndex'] + \
             ' -d ' + es_info['docType'] + \
             ' -s ' + es_server

    elif 'BING' in session['search_engine']:
      comm = 'java -cp target/seeds_generator-1.0-SNAPSHOT-jar-with-dependencies.jar BingSearch -t ' + str(top) + \
             ' -q "' + terms.replace('"','\\"') + '"' + \
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

  def uploadUrls(self, urls_str, session):
    """ Download pages corresponding to already known set of domain URLs

    Parameters:
        urls_str (string): Space separated list of URLs
        session (json): should have domainId

    Returns:
        number of pages downloaded (int)

    """
    es_info = self._esInfo(session['domainId'])

    output = callDownloadUrls("uploaded", None, urls_str, es_info)

    return output

  def getForwardLinks(self, urls, session):
    """ The content can be extended by crawling the given pages one level forward. The assumption here is that a relevant page will contain links to other relevant pages.

    Parameters:
        urls (list): list of urls to crawl forward
        session (json): should have domainId

    Return:
        None (Results are downloaded into elasticsearch)
    """
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

    data_dir = self._path + "/data/"
    data_domain  = data_dir + es_info['activeDomainIndex']

    domainmodel_dir = data_domain + "/models/"

    if (not isfile(domainmodel_dir+"pageclassifier.model")):
      self.createModel(session, zip=False)

    print "\n\n\n RUN SEED FINDER",terms,"\n\n\n"

    # Execute SeedFinder in a new thread
    if self.runningSeedFinders.get(terms) is not None:
      return self.runningSeedFinders[terms]['status']

    self.runningSeedFinders[terms] = {"domain": self._domains[domainId]['domain_name'], "status": "Running", "description":"Query: "+terms}  
    p = self.pool.submit(self.seedfinder.execSeedFinder, terms, self._path, es_info)
    p.add_done_callback(self._seedFinderDone)
    self.runningSeedFinders[terms]["process"] = p
    
    return "Running"

  def _seedFinderDone(self, p):
    for k,v in self.runningSeedFinders.items():
      if v["process"] == p:
        self.runningSeedFinders[k]["status"] = "Completed"
        print "\n\n\n SeedFinder ", k, " Completed \n\n\n"
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
    results = get_documents(pages, 'url', [es_info['mapping']['tag']], es_info['activeDomainIndex'], es_info['docType'],  self._es)

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

    s_fields = {
      "term": "",
      "index": es_info['activeDomainIndex'],
      "doc_type": es_info['docType'],
    }

    tags = []
    for term in terms:
      s_fields["term"] = term
      res = multifield_term_search(s_fields, 0, 1, ['tag'], self._termsIndex, 'terms', self._es)
      tags.extend(res["results"])

    results = {result['id']: result['tag'][0] for result in tags}

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
          if not results.get(term) is None:
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
    print "Time to get pos and neg terms = ", end-start

    start = time.time()
    
    # Get selected pages displayed in the MDS window
    session["from"] = 0
    results = self._getPagesQuery(session)

    end = time.time()
    print "Time to get query pages = ", end-start

    top_terms = []

    start = time.time()
    
    text = []
    url_ids = [hit["id"] for hit in results["results"]][0:MAX_SAMPLE_PAGES]
    if(len(url_ids) > 0):
      results = get_documents_by_id(url_ids, [es_info['mapping']["url"], es_info['mapping']["text"]], es_info["activeDomainIndex"], es_info["docType"], self._es)
      text = [" ".join(hit[es_info['mapping']["text"]][0].split(" ")[0:MAX_TEXT_LENGTH]) for hit in results]

      end = time.time()
      print "Time to get text for 500 query pages = ", end-start

    if len(url_ids) > 0 and text:

      start = time.time()
      tfidf_v = tfidf_vectorizer(max_features=opt_maxNumberOfTerms+len(neg_terms), ngram_range=(1,3))
      tfidf_v.tfidf(text)
      if pos_terms:
        extract_terms_all = extract_terms.extract_terms(tfidf_v)
        [ranked_terms, scores] = extract_terms_all.results(pos_terms)
        top_terms = [ term for term in ranked_terms if (term not in neg_terms)]
        top_terms = top_terms[0:opt_maxNumberOfTerms]
        end = time.time()
        print "Time to rank top terms by Bayesian sets = ", end-start
      else:
        top_terms = [term for term in tfidf_v.getTopTerms(opt_maxNumberOfTerms+len(neg_terms)) if (term not in neg_terms)]
        end = time.time()
        print "Time to get top terms by sorting tfidf= ", end-start

    start = time.time()
    
    s_fields = {
      "tag": "Custom",
      "index": es_info['activeDomainIndex'],
      "doc_type": es_info['docType']
    }

    results = multifield_query_search(s_fields, 0, 500, ['term'], self._termsIndex, 'terms', self._es)
    custom_terms = [field['term'][0] for field in results["results"]]

    for term in custom_terms:
      try:
        top_terms = top_terms.remove(term)
      except ValueError:
        continue

    if not top_terms:
      return []

    end = time.time()
    print "Time to remove already annotated terms = ", end-start

    start = time.time()
    
    results = term_search(es_info['mapping']['tag'], ['Relevant'], 0, self._all, ['url', es_info['mapping']['text']], es_info['activeDomainIndex'], es_info['docType'], self._es)
    pos_data = {field['id']:" ".join(field[es_info['mapping']['text']][0].split(" ")[0:MAX_TEXT_LENGTH]) for field in results["results"]}
    pos_urls = pos_data.keys();
    pos_text = pos_data.values();

    total_pos_tf = None
    total_pos = 0

    pos_corpus = []

    if len(pos_urls) > 1:
      tfidf_pos = tfidf_vectorizer(ngram_range=(1,3))
      [_, total_pos_tf, pos_corpus] = tfidf_pos.tfidf(pos_text)
      total_pos_tf = np.sum(total_pos_tf, axis=0)
      total_pos = np.sum(total_pos_tf)
      total_pos_tf = total_pos_tf.flatten().tolist()[0]

    results = term_search(es_info['mapping']['tag'], ['Irrelevant'], 0, self._all, ['url', es_info['mapping']['text']], es_info['activeDomainIndex'], es_info['docType'], self._es)
    neg_data = {field['id']:" ".join(field[es_info['mapping']['text']][0].split(" ")[0:MAX_TEXT_LENGTH]) for field in results["results"]}
    neg_urls = neg_data.keys();
    neg_text = neg_data.values();

    total_neg_tf = None
    total_neg = 0

    neg_corpus = []

    if len(neg_urls) > 1:
      tfidf_neg = tfidf_vectorizer(ngram_range=(1,3))
      [_, total_neg_tf, neg_corpus] = tfidf_neg.tfidf(neg_text)
      total_neg_tf = np.sum(total_neg_tf, axis=0)
      total_neg = np.sum(total_neg_tf)
      total_neg_tf = total_neg_tf.flatten().tolist()[0]

    entry = {}
    for key in top_terms + [term for term in custom_terms if term in pos_corpus or term in neg_corpus]:
      if key in pos_corpus:
        if total_pos != 0:
          entry[key] = {"pos_freq": (float(total_pos_tf[pos_corpus.index(key)])/total_pos)}
        else:
          entry[key] = {"pos_freq": 0}
      else:
        entry[key] = {"pos_freq": 0}

      if key in neg_corpus:
        if total_neg != 0:
          entry[key].update({"neg_freq": (float(total_neg_tf[neg_corpus.index(key)])/total_neg)})
        else:
          entry[key].update({"neg_freq": 0})
      else:
        entry[key].update({"neg_freq": 0})

      if key in pos_terms:
        entry[key].update({"tag": ["Positive"]})
      elif key in neg_terms:
        entry[key].update({"tag": ["Negative"]})
      else:
        entry[key].update({"tag": []})

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
    print "Time to compute terms in pos and neg pages = ", end-start
    
    terms = [[key, entry[key]["pos_freq"], entry[key]["neg_freq"], entry[key]["tag"]] for key in custom_terms + top_terms] # + top_bigrams + top_trigrams

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

      if not hit.get('url') is None:
        doc[0] = hit.get('url')
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

    session['pagesCap'] = 12

    if session.get('from') is None:
      session['from'] = 0

    format = '%m/%d/%Y %H:%M %Z'
    if not session.get('fromDate') is None:
      session['fromDate'] = long(DomainModel.convert_to_epoch(datetime.strptime(session['fromDate'], format)))

    if not session.get('toDate') is None:
      session['toDate'] = long(DomainModel.convert_to_epoch(datetime.strptime(session['toDate'], format)))

    results = self._getPagesQuery(session)

    hits = results['results']

    no_image_desc_ids = Set()
    docs = {}
    for hit in hits:
      doc = {}
      if not hit.get('description') is None:
        doc["snippet"] = " ".join(hit['description'][0].split(" ")[0:20])
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
        doc["timestamp"] = hit[es_info['mapping']["timestamp"]]

      if(not hit.get('url') is None):
        docs[hit['url'][0]] = doc
      else:
        print "\n\n\n Could not find URL for ",hit["id"],"\n\n\n"

    if len(no_image_desc_ids) > 0:

      image_desc_hits = get_documents_by_id(list(no_image_desc_ids), ["url", "html", "text"], es_info['activeDomainIndex'], es_info['docType'], self._es)

      for image_desc_hit in image_desc_hits:
        if image_desc_hit.get('html') is not None:
          imageURL = getImage(image_desc_hit["html"][0], image_desc_hit['url'][0])

          if imageURL is not None:
            docs[image_desc_hit['url'][0]]['image_url'] = imageURL

          desc = getDescription(image_desc_hit["html"][0], image_desc_hit['text'][0])
          if desc is not None:
            docs[image_desc_hit['url'][0]]['snippet'] =  " ".join(desc.split(" ")[0:20])

    return {'total': results['total'], 'results': docs}

  def _getMostRecentPages(self, session):
    es_info = self._esInfo(session['domainId'])

    hits = []
    if session['fromDate'] is None and session['filter'] is None:
      hits = get_most_recent_documents(session['from'], session['pagesCap'],
                                       es_info['mapping'],
                                       ["url", "description", "image_url", "title", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"], es_info['mapping']["text"]],
                                       session['filter'],
                                       es_info['activeDomainIndex'],
                                       es_info['docType'],
                                       self._es)
    elif not session['fromDate'] is None and session['filter'] is None:
        hits = range_search(es_info['mapping']["timestamp"], session['fromDate'], session['toDate'], ["url", "description", "image_url", "title", "x", "y", es_info['mapping']['tag'], es_info['mapping']["timestamp"], es_info['mapping']["text"]], True, session['from'], session['pagesCap'],
                            es_info['activeDomainIndex'],
                            es_info['docType'],
                            self._es)
    elif not session['filter'] is None:
        s_fields = {}
        if not session['fromDate'] is None:
          es_info['mapping']["timestamp"] = "[" + str(session['fromDate']) + " TO " + str(session['toDate']) + "]"
        s_fields['multi_match'] = [[session['filter'].replace('"','\"'), [es_info['mapping']["text"], es_info['mapping']["title"]+"^2",es_info['mapping']["domain"]+"^3"]]]
        hits = multifield_term_search(s_fields,
                                      session['from'], session['pagesCap'],
                                      ["url", "description", "image_url", "title", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"], es_info['mapping']["text"]],
                                      es_info['activeDomainIndex'],
                                      es_info['docType'],
                                      self._es)
    return hits

  def _getPagesForMultiCriteria(self, session):
    es_info = self._esInfo(session['domainId'])

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
          if criterion in 'Maybe relevant':
            filters.append({"term":{"label_pos": 1}})
          elif criterion in 'Maybe irrelevant':
            filters.append({"term":{"label_neg": 1}})
          elif criterion in 'Unsure':
            filters.append({"term":{"unsure_tag": 1}})
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
                         ["url", "description", "image_url", "title", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"], es_info['mapping']["text"]],
                         session['from'], session['pagesCap'], 
                         es_info['activeDomainIndex'],
                         es_info['docType'],
                         self._es)

    # if session['selected_morelike']=="moreLike":
    #   morelike_result = self._getMoreLikePagesAll(session, results['results'])
    #   hits['results'].extend(morelike_result)
    # else:
    #   hits.extend(results)

    return results

  def _getPagesForQueries(self, session):
    es_info = self._esInfo(session['domainId'])

    s_fields = {}
    if not session['filter'] is None:
      s_fields['multi_match'] = [[session['filter'].replace('"','\"'), [es_info['mapping']["text"], es_info['mapping']["title"]+"^2",es_info['mapping']["domain"]+"^3"]]]

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
                                    ["url", "description", "image_url", "title", "rank", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"]],
                                    es_info['activeDomainIndex'],
                                    es_info['docType'],
                                    self._es)

    #TODO: Revisit when allowing selected_morelike
    # if session['selected_morelike']=="moreLike":
    #   aux_result = self._getMoreLikePagesAll(session, results)
    #   hits.extend(aux_result)
    # else:
    #   hits.extend(results)

    return results

  def _getPagesForTLDs(self, session):
    es_info = self._esInfo(session['domainId'])

    s_fields = {}
    if not session['filter'] is None:
      s_fields['multi_match'] = [[session['filter'].replace('"','\"'), [es_info['mapping']["text"], es_info['mapping']["title"]+"^2",es_info['mapping']["domain"]+"^3"]]]

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
                                       ["url", "description", "image_url", "title", "rank", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"]],
                                       es_info['activeDomainIndex'],
                                       es_info['docType'],
                                       self._es)
      # if session['selected_morelike']=="moreLike":
      #   aux_result = self._getMoreLikePagesAll(session, results)
      #   hits.extend(aux_result)
      # else:
      #   hits.extend(results)

    return results

  def _getPagesForTags(self, session):
    es_info = self._esInfo(session['domainId'])

    s_fields = {}
    if not session['filter'] is None:
      s_fields['multi_match'] = [[session['filter'].replace('"','\"'), [es_info['mapping']["text"], es_info['mapping']["title"]+"^2",es_info['mapping']["domain"]+"^3"]]]

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

    results = multifield_term_search(s_fields, session['from'], session['pagesCap'], ["url", "description", "image_url", "title", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"], es_info['mapping']["text"]],
                                     es_info['activeDomainIndex'],
                                    es_info['docType'],
                                     self._es)

    return results

  def _getPagesForCrawledTags(self, session):
    es_info = self._esInfo(session['domainId'])

    s_fields = {}
    if not session['filter'] is None:
      s_fields['multi_match'] = [[session['filter'].replace('"','\"'), [es_info['mapping']["text"], es_info['mapping']["title"]+"^2",es_info['mapping']["domain"]+"^3"]]]      

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

    results = multifield_term_search(s_fields, session['from'], session['pagesCap'], ["url", "description", "image_url", "title", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"], es_info['mapping']["text"]],
                                     es_info['activeDomainIndex'],
                                    es_info['docType'],
                                     self._es)

    return results

  def _getPagesForModelTags(self, session):
    es_info = self._esInfo(session['domainId'])

    s_fields = {}
    if not session['filter'] is None:
      s_fields['multi_match'] = [[session['filter'].replace('"','\"'), [es_info['mapping']["text"], es_info['mapping']["title"]+"^2",es_info['mapping']["domain"]+"^3"]]]      

    filters=[]
    tags = session['selected_model_tags'].split(',')

    for tag in tags:
      if tag == "Unsure":
        filters.append({"term":{"unsure_tag":1}})
      elif tag == "Maybe relevant":
        filters.append({"term":{"label_pos":1}})
      elif tag == "Maybe irrelevant":
        filters.append({"term":{"label_neg":1}})

    if len(filters) > 0:
      s_fields["filter"] = {"or":filters}

    results = multifield_term_search(s_fields, session['from'], session['pagesCap'], ["url", "description", "image_url", "title", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"], es_info['mapping']["text"]],
                                     es_info['activeDomainIndex'],
                                    es_info['docType'],
                                     self._es)

    return results

  def _getRelevantPages(self, session):
    es_info = self._esInfo(session['domainId'])

    pos_hits = search(es_info['mapping']['tag'], ['relevant'], session['from'], session['pagesCap'], ["url", "description", "image_url", "title", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"], es_info['mapping']["text"]], es_info['activeDomainIndex'], 'page', self._es)

    return pos_hits

  def _getMoreLikePages(self, session):
    es_info = self._esInfo(session['domainId'])

    hits=[]
    tags = session['selected_tags'].split(',')
    for tag in tags:
      tag_hits = search(es_info['mapping']['tag'], [tag], session['from'], session['pagesCap'], ["url", "description", "image_url", "title", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"], es_info['mapping']["text"]], es_info['activeDomainIndex'], 'page', self._es)

      if len(tag_hits) > 0:
        tag_urls = [field['id'] for field in tag_hits]

        results = get_more_like_this(tag_urls, ["url", "description", "image_url", "title", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"], es_info['mapping']["text"]], session['from'], session['from'], session['pagesCap'],  es_info['activeDomainIndex'], es_info['docType'],  self._es)

        hits.extend(tag_hits[0:self._pagesCapTerms] + results)

    return hits

  def _getMoreLikePagesAll(self, session, tag_hits):
      es_info = self._esInfo(session['domainId'])
      if len(tag_hits) > 0:
          tag_urls = [field['id'] for field in tag_hits]
          results = get_more_like_this(tag_urls, ["url", "description", "image_url", "title", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"], es_info['mapping']["text"]], session['pagesCap'],  es_info['activeDomainIndex'], es_info['docType'],  self._es)
          aux_result = tag_hits[0:self._pagesCapTerms] + results
      else: aux_result=tag_hits
      return aux_result


  def _getUnsureLabelPages(self, session):
    es_info = self._esInfo(session['domainId'])
    unsure_label_hits = term_search("unsure_tag", "1", session['from'], session["pagesCap"], ["url", "description", "image_url", "title", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"], es_info['mapping']["text"]], es_info['activeDomainIndex'], es_info['docType'], self._es)

    return unsure_label_hits

  def _getPosLabelPages(self, session):
    es_info = self._esInfo(session['domainId'])

    pos_label_hits = term_search("label_pos", "1", session['from'], session["pagesCap"], ["url", "description", "image_url", "title", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"], es_info['mapping']["text"]], es_info['activeDomainIndex'], es_info['docType'], self._es)

    return pos_label_hits

  def _getNegLabelPages(self, session):
    es_info = self._esInfo(session['domainId'])

    neg_label_hits = term_search("label_neg", "1", session['from'], session["pagesCap"], ["url", "description", "image_url", "title", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"], es_info['mapping']["text"]], es_info['activeDomainIndex'], es_info['docType'], self._es)

    return neg_label_hits

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



#######################################################################################################
# Generate Model
#######################################################################################################

  def createModel(self, session, zip=True):
    """ Create an ACHE model to be applied to SeedFinder and focused crawler.
    It saves the classifiers, features, the training data in the <project>/data/<domain> directory.
    If zip=True all generated files and folders are zipped into a file.

    Parameters:
        session (json): should have domainId

    Returns:
        None
    """
    es_info = self._esInfo(session['domainId']);

    data_dir = self._path + "/data/"
    data_domain  = data_dir + es_info['activeDomainIndex']
    data_training = data_domain + "/training_data/"
    data_negative = data_domain + "/training_data/negative/"
    data_positive = data_domain + "/training_data/positive/"

    if (not isdir(data_positive)):
      # Create dir if it does not exist
      makedirs(data_positive)
    else:
      # Remove all previous files
      for filename in os.listdir(data_positive):
        os.remove(data_positive+filename)

    if (not isdir(data_negative)):
      # Create dir if it does not exist
      makedirs(data_negative)
    else:
      # Remove all previous files
      for filename in os.listdir(data_negative):
        os.remove(data_negative+filename)

    pos_tags = "Relevant"
    neg_tags = "Irrelevant"
    try:
      pos_tags = session['model']['positive']
    except KeyError:
      print "Using default positive tags"

    try:
      neg_tags = session['model']['negative']
    except KeyError:
      print "Using default negative tags"

    pos_docs = []
    for tag in pos_tags.split(','):
      s_fields = {}
      query = {
        "wildcard": {es_info['mapping']["tag"]:tag}
      }
      s_fields["queries"] = [query]

      results = multifield_term_search(s_fields,
                                       0, self._all,
                                       ["url", es_info['mapping']['html']],
                                       es_info['activeDomainIndex'],
                                       es_info['docType'],
                                       self._es)
      
      pos_docs = pos_docs + results['results']
      
    neg_docs = []
    for tag in neg_tags.split(','):
      s_fields = {}
      query = {
        "wildcard": {es_info['mapping']["tag"]:tag}
      }
      s_fields["queries"] = [query]
      results = multifield_term_search(s_fields,
                                       0, self._all,
                                       ["url", es_info['mapping']['html']],
                                       es_info['activeDomainIndex'],
                                       es_info['docType'],
                                       self._es)
      neg_docs = neg_docs + results['results']

    pos_html = {field['url'][0]:field[es_info['mapping']["html"]][0] for field in pos_docs}
    neg_html = {field['url'][0]:field[es_info['mapping']["html"]][0] for field in neg_docs}

    seeds_file = data_domain +"/seeds.txt"
    print "Seeds path ", seeds_file
    with open(seeds_file, 'w') as s:
      for url in pos_html:
        try:
          file_positive = data_positive + self._encode(url.encode('utf8'))
          s.write(url.encode('utf8') + '\n')
          with open(file_positive, 'w') as f:
            f.write(pos_html[url])

        except IOError:
          _, exc_obj, tb = exc_info()
          f = tb.tb_frame
          lineno = tb.tb_lineno
          filename = f.f_code.co_filename
          linecache.checkcache(filename)
          line = linecache.getline(filename, lineno, f.f_globals)
          print 'EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj)

    for url in neg_html:
      try:
        file_negative = data_negative + self._encode(url.encode('utf8'))
        with open(file_negative, 'w') as f:
          f.write(neg_html[url])
      except IOError:
        _, exc_obj, tb = exc_info()
        f = tb.tb_frame
        lineno = tb.tb_lineno
        filename = f.f_code.co_filename
        linecache.checkcache(filename)
        line = linecache.getline(filename, lineno, f.f_globals)
        print 'EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj)

    domainmodel_dir = data_domain + "/models/"

    if (not isdir(domainmodel_dir)):
      makedirs(domainmodel_dir)

    ache_home = environ['ACHE_HOME']
    comm = ache_home + "/bin/ache buildModel -t " + data_training + " -o "+ domainmodel_dir + " -c " + ache_home + "/config/sample_config/stoplist.txt"
    p = Popen(comm, shell=True, stderr=PIPE)
    output, errors = p.communicate()
    print output
    print errors

    if zip:
      print data_dir
      print es_info['activeDomainIndex']
      zip_dir = data_dir
      #Create tha model in the client (client/build/models/). Just the client site is being exposed
      saveClientSite = zip_dir.replace('server/data/','client/build/models/')
      if (not isdir(saveClientSite)):
        makedirs(saveClientSite)
      zip_filename = saveClientSite + es_info['activeDomainIndex'] + "_model.zip"

      with ZipFile(zip_filename, "w") as modelzip:
        if (isfile(domainmodel_dir + "/pageclassifier.features")):
          print "zipping file: "+domainmodel_dir + "/pageclassifier.features"
          modelzip.write(domainmodel_dir + "/pageclassifier.features", "pageclassifier.features")

        if (isfile(domainmodel_dir + "/pageclassifier.model")):
          print "zipping file: "+domainmodel_dir + "/pageclassifier.model"
          modelzip.write(domainmodel_dir + "/pageclassifier.model", "pageclassifier.model")

        if (exists(data_domain + "/training_data/positive")):
          print "zipping file: "+ data_domain + "/training_data/positive"
          for (dirpath, dirnames, filenames) in walk(data_domain + "/training_data/positive"):
            for html_file in filenames:
              modelzip.write(dirpath + "/" + html_file, "training_data/positive/" + html_file)

        if (exists(data_domain + "/training_data/negative")):
          print "zipping file: "+ data_domain + "/training_data/negative"
          for (dirpath, dirnames, filenames) in walk(data_domain + "/training_data/negative"):
            for html_file in filenames:
              modelzip.write(dirpath + "/" + html_file, "training_data/negative/" + html_file)

        if (isfile(data_domain +"/seeds.txt")):
          print "zipping file: "+data_domain +"/seeds.txt"
          modelzip.write(data_domain +"/seeds.txt", es_info['activeDomainIndex'] + "_seeds.txt")
        chmod(zip_filename, 0o777)

      return "models/" + es_info['activeDomainIndex'] + "_model.zip"
    else:
      return None


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

    results = exec_query(query, ["url", es_info['mapping']['text']],
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
    results = exec_query(query, ["url", es_info['mapping']['text']],
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
                                               ["url", es_info['mapping']['text']],
                                               es_info['activeDomainIndex'],
                                               es_info['docType'],
                                               self._es)
        pos_trained_text = [pos_trained_doc[es_info['mapping']['text']][0][0:MAX_TEXT_LENGTH] for pos_trained_doc in pos_trained_docs]
        pos_trained_labels = [1 for i in range(0, len(pos_trained_text))]

        neg_trained_docs = get_documents_by_id(trainedNegSamples,
                                               ["url", es_info['mapping']['text']],
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


  def predictUnlabeled(self, session):
    # Label unlabelled data

    #TODO: Move this to Model tab functionality
    es_info = self._esInfo(session['domainId'])

    #self.updateOnlineClassifier(session)

    unsure = 0
    label_pos = 0
    label_neg = 0
    unlabeled_urls = []

    MAX_SAMPLE = 500

    if self._onlineClassifiers.get(session['domainId']) == None:
      return

    sigmoid = self._onlineClassifiers[session['domainId']].get("sigmoid")
    if sigmoid != None:
      unlabelled_docs = field_missing(es_info["mapping"]["tag"], [es_info['mapping']['url']], self._all,
                                      es_info['activeDomainIndex'],
                                      es_info['docType'],
                                      self._es)

      if len(unlabelled_docs) > MAX_SAMPLE:
        unlabelled_docs = sample(unlabelled_docs, MAX_SAMPLE)

      unlabelled_docs_ids = [doc["id"] for doc in unlabelled_docs]

      unlabelled_docs = get_documents_by_id(unlabelled_docs_ids, [es_info['mapping']['url'], es_info['mapping']['text']],
                                            es_info['activeDomainIndex'],
                                            es_info['docType'],
                                            self._es)

      unlabeled_text = [unlabelled_doc[es_info['mapping']['text']][0][0:MAX_TEXT_LENGTH] for unlabelled_doc in unlabelled_docs if unlabelled_doc.get(es_info['mapping']['text']) is not None]

      # Check if unlabeled data available
      if len(unlabeled_text) > 0:
        unlabeled_urls = [unlabelled_doc[es_info['mapping']['url']][0] for unlabelled_doc in unlabelled_docs if unlabelled_doc.get(es_info['mapping']['url']) is not None]
        unlabeled_ids = [unlabelled_doc["id"] for unlabelled_doc in unlabelled_docs]

        [unlabeled_data,_] =  self._onlineClassifiers[session['domainId']]["onlineClassifier"].vectorize(unlabeled_text)
        [classp, calibp, cm] = self._onlineClassifiers[session['domainId']]["onlineClassifier"].predictClass(unlabeled_data,sigmoid)

        pos_calib_indices = np.nonzero(calibp)
        neg_calib_indices = np.where(calibp == 0)

        pos_cm = [cm[pos_calib_indices][i][1] for i in range(0,np.shape(cm[pos_calib_indices])[0])]
        neg_cm = [cm[neg_calib_indices][i][0] for i in range(0,np.shape(cm[neg_calib_indices])[0])]

        pos_sorted_cm = pos_calib_indices[0][np.asarray(np.argsort(pos_cm)[::-1])]
        neg_sorted_cm = neg_calib_indices[0][np.asarray(np.argsort(neg_cm)[::-1])]

        entries = {}
        for i in pos_sorted_cm:
          entry = {}
          if cm[i][1] < 60:
            entry["unsure_tag"] = 1
            entry["label_pos"] = 0
            entry["label_neg"] = 0
            entries[unlabeled_ids[i]] = entry
            unsure = unsure + 1
          else:
            entry["label_pos"] = 1
            entry["unsure_tag"] = 0
            entry["label_neg"] = 0
            entries[unlabeled_ids[i]] = entry

            label_pos = label_pos + 1

        for i in neg_sorted_cm:
          entry = {}
          if cm[i][0] < 60:
            entry["unsure_tag"] = 1
            entry["label_pos"] = 0
            entry["label_neg"] = 0
            entries[unlabeled_ids[i]] = entry

            unsure = unsure + 1
          else:
            entry["label_neg"] = 1
            entry["unsure_tag"] = 0
            entry["label_pos"] = 0

            entries[unlabeled_ids[i]] = entry
            label_neg = label_neg + 1

        if entries:
          update_try = 0
          while (update_try < 10):
            try:
              update_document(entries, es_info['activeDomainIndex'], es_info['docType'], self._es)
              break
            except:
              update_try = update_try + 1

        pos_indices = np.nonzero(classp)
        neg_indices = np.where(classp == 0)

    return {"Unsure": unsure, "Maybe relevant": label_pos, "Maybe irrelevant": label_neg}

#######################################################################################################
# Run Crawler
#######################################################################################################

  def startCrawler(self, session):
    """ Start the ACHE crawler for the specfied domain with the domain model. The
    results are stored in the same index

    Parameters:
    session (json): should have domainId

    Returns:
    None
    """

    domainId = session['domainId']

    if self.runningCrawlers.get(domainId) is not None:
      return self.runningCrawlers[domainId]['status']

    if len(self.runningCrawlers.keys()) == 0:
      es_info = self._esInfo(domainId)

      data_dir = self._path + "/data/"
      data_domain  = data_dir + es_info['activeDomainIndex']
      domainmodel_dir = data_domain + "/models/"
      domainoutput_dir = data_domain + "/output/"

      if (not isdir(domainmodel_dir)):
        self.createModel(session, False)
      if (not isdir(domainmodel_dir)):
        return "No domain model available"

      ache_home = environ['ACHE_HOME']
      comm = ache_home + "/bin/ache startCrawl -c " + self._path + " -e " + es_info['activeDomainIndex'] + " -t " + es_info['docType']  + " -m " + domainmodel_dir + " -o " + domainoutput_dir + " -s " + data_domain + "/seeds.txt"
      p = Popen(shlex.split(comm))
      self.runningCrawlers[domainId] = {'process': p, 'domain': self._domains[domainId]['domain_name'], 'status': "Crawler is running" }

      return "Crawler is running"
    return "Crawler running for domain: " + self._domains[self.runningCrawlers.keys()[0]]['domain_name']

  def stopCrawler(self, session):
    """ Stop the ACHE crawler for the specfied domain with the domain model. The
    results are stored in the same index

    Parameters:
    session (json): should have domainId

    Returns:
    None
    """

    domainId = session['domainId']

    p = self.runningCrawlers[domainId]['process']

    p.terminate()

    print "\n\n\nSHUTTING DOWN\n\n\n"

    self.runningCrawlers[domainId]['status'] = "Crawler shutting down"

    p.wait()

    self.runningCrawlers.pop(domainId)

    print "\n\n\nCrawler Stopped\n\n\n"
    return "Crawler Stopped"

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
