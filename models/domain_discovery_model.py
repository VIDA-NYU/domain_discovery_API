import time
import calendar
import os
from datetime import datetime
from dateutil import tz
from sets import Set
from itertools import product
import json;

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

from seeds_generator.download import callDownloadUrls
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

#from topik import read_input, tokenize, vectorize, run_model, visualize, TopikProject

from concurrent.futures import ThreadPoolExecutor as Pool

import urllib2

MAX_TEXT_LENGTH = 3000
MAX_TERM_FREQ = 2
MAX_LABEL_PAGES = 2000

class DomainModel(object):

  w2v = word2vec.word2vec(from_es=False)

  def __init__(self):
    self._es = None
    self._all = 10000
    self._termsIndex = "ddt_terms"
    self._pagesCapTerms = 100
    self._capTerms = 500
    self.projectionsAlg = {'Group by Similarity': self.pca,
                           'Group by Correlation': self.tsne
                           # 'K-Means': self.kmeans,
                         }

    create_config_index()
    create_terms_index()

    self._mapping = {"url":"url", "timestamp":"retrieved", "text":"text", "html":"html", "tag":"tag", "query":"query", "domain":"domain"}
    self._domains = None
    self._onlineClassifiers = {}
    self._pos_tags = ['NN', 'NNS', 'NNP', 'NNPS', 'FW', 'JJ']
    self._path = ""


    self.results_file = open("results.txt", "w")

    self.pool = Pool(max_workers=3)
    self.seedfinder = RunSeedFinder()

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
    
  def setPath(self, path):
    self._path = path

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
    return get_unique_values('query', self._all, es_info['activeDomainIndex'], es_info['docType'], self._es)

  def getAvailableTags(self, session):
    """ Return all tags for the selected domain.

    Parameters:
        session (json): Should contain the domainId

    Returns:
        json: {<tag>: <number of pages for the tag>}

    """

    es_info = self._esInfo(session['domainId'])

    tags_neutral = field_missing("tag", ["url"], self._all, es_info['activeDomainIndex'], es_info['docType'], self._es)
    unique_tags = {"Neutral": len(tags_neutral)}

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

    unsure_tags = self._getUnsureLabelPages(session)
    unique_tags = {"Unsure": len(unsure_tags)}

    relevant_tags = self._getPosLabelPages(session)
    unique_tags["Maybe relevant"] = len(relevant_tags)

    irrelevant_tags = self._getNegLabelPages(session)
    unique_tags["Maybe irrelevant"] = len(irrelevant_tags)

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

    tags = multifield_term_search(s_fields, self._capTerms, ['tag'], self._termsIndex, 'terms', self._es)

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

    hits = multifield_term_search(s_fields, self._all, ['tag','term'], self._termsIndex, 'terms', self._es)

    print hits
    
    result = {}
    for hit in hits:
      term = hit['term'][0]
      result[term] = {'tag':hit['tag'][0]}
      
    print "\n\n\n GET ANNOTATED TERMs", result.keys(),"\n\n\n"
    return result
                      
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
      ddt_terms_keys = [doc["id"] for doc in term_search("index", [index], self._all, ["term"], "ddt_terms", "terms", self._es)]
      delete_document(ddt_terms_keys, "ddt_terms", "terms", self._es)

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
      comm = "java -cp target/seeds_generator-1.0-SNAPSHOT-jar-with-dependencies.jar GoogleSearch -t " + str(top) + \
             " -q \"" + terms + "\"" + \
             " -i " + es_info['activeDomainIndex'] + \
             " -d " + es_info['docType'] + \
             " -s " + es_server

    elif 'BING' in session['search_engine']:
      comm = "java -cp target/seeds_generator-1.0-SNAPSHOT-jar-with-dependencies.jar BingSearch -t " + str(top) + \
             " -q \"" + terms + "\"" + \
             " -i " + es_info['activeDomainIndex'] + \
             " -d " + es_info['docType'] + \
             " -s " + es_server


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
    es_info = self._esInfo(session['domainId']);

    data_dir = self._path + "/data/"
    data_domain  = data_dir + es_info['activeDomainIndex']

    domainmodel_dir = data_domain + "/models/"

    if (not isfile(domainmodel_dir+"pageclassifier.model")):
      self.createModel(session, zip=False)

    print "\n\n\n RUN SEED FINDER",terms,"\n\n\n"

    # Execute SeedFinder in a new thread
    p = self.pool.submit(self.seedfinder.execSeedFinder, terms, self._path, es_info)


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
      res = multifield_term_search(s_fields, 1, ['tag'], self._termsIndex, 'terms', self._es)
      tags.extend(res)

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

    pos_terms = [field['term'][0] for field in multifield_term_search(s_fields, self._capTerms, ['term'], self._termsIndex, 'terms', self._es)]

    s_fields["tag"]="Negative"
    neg_terms = [field['term'][0] for field in multifield_term_search(s_fields, self._capTerms, ['term'], self._termsIndex, 'terms', self._es)]

    # Get selected pages displayed in the MDS window
    results = self._getPagesQuery(session)

    top_terms = []
    top_bigrams = []
    top_trigrams = []

    text = []
    urls = [hit["id"] for hit in results if (hit.get(es_info['mapping']["tag"]) is not None) and ("Relevant" in hit[es_info['mapping']["tag"]])]
    if(len(urls) > 0):
      text = [" ".join(hit[es_info['mapping']["text"]][0].split(" ")[0:MAX_TEXT_LENGTH]) for hit in results if (hit.get(es_info['mapping']["tag"]) is not None) and ("Relevant" in hit[es_info['mapping']["tag"]])]
    else:
      urls = [hit["id"] for hit in results]
      # If positive urls are not available then get the most recent documents
      text = [" ".join(hit[es_info['mapping']["text"]][0].split(" ")[0:MAX_TEXT_LENGTH]) for hit in results if hit[es_info['mapping']["text"]][0] != ""]

    if session["filter"] == "" or session["filter"] is None:
      if len(urls) > 0 and text:
        [bigram_tfidf_data, trigram_tfidf_data,_,_,bigram_corpus, trigram_corpus,top_bigrams, top_trigrams] = get_bigrams_trigrams.get_bigrams_trigrams(text, opt_maxNumberOfTerms+len(neg_terms), self._es)

        tfidf_all = tfidf.tfidf(urls, pos_tags=self._pos_tags, mapping=es_info['mapping'], es_index=es_info['activeDomainIndex'], es_doc_type=es_info['docType'], es=self._es)
        if pos_terms:
          extract_terms_all = extract_terms.extract_terms(tfidf_all)
          [ranked_terms, scores] = extract_terms_all.results(pos_terms)
          top_terms = [ term for term in ranked_terms if (term not in neg_terms)]
          top_terms = top_terms[0:opt_maxNumberOfTerms]

          tfidf_bigram = tfidf.tfidf()
          tfidf_bigram.tfidfArray = bigram_tfidf_data
          tfidf_bigram.opt_docs = urls
          tfidf_bigram.corpus = bigram_corpus
          tfidf_bigram.mapping = es_info['mapping']
          extract_terms_all = extract_terms.extract_terms(tfidf_bigram)
          [ranked_terms, scores] = extract_terms_all.results(pos_terms)
          top_bigrams = [ term for term in ranked_terms if (term not in neg_terms)]

          tfidf_trigram = tfidf.tfidf()
          tfidf_trigram.tfidfArray = trigram_tfidf_data
          tfidf_trigram.opt_docs = urls
          tfidf_trigram.corpus = trigram_corpus
          tfidf_trigram.mapping = es_info['mapping']
          extract_terms_all = extract_terms.extract_terms(tfidf_trigram)
          [ranked_terms, scores] = extract_terms_all.results(pos_terms)
          top_trigrams = [ term for term in ranked_terms if (term not in neg_terms)]
          top_trigrams = top_trigrams[0:opt_maxNumberOfTerms]
        else:
          top_terms = [term for term in tfidf_all.getTopTerms(opt_maxNumberOfTerms+len(neg_terms)) if (term not in neg_terms)]
          top_bigrams = [term for term in top_bigrams if term not in neg_terms]
          top_trigrams = [term for term in top_trigrams if term not in neg_terms]
    else:
      top_terms = [term for term in get_significant_terms(urls, opt_maxNumberOfTerms+len(neg_terms), mapping=es_info['mapping'], es_index=es_info['activeDomainIndex'], es_doc_type=es_info['docType'], es=self._es) if (term not in neg_terms)]
      if len(text) > 0:
        [_,_,_,_,_,_,top_bigrams, top_trigrams] = get_bigrams_trigrams.get_bigrams_trigrams(text, opt_maxNumberOfTerms+len(neg_terms), self._es)
        top_bigrams = [term for term in top_bigrams if term not in neg_terms]
        top_trigrams = [term for term in top_trigrams if term not in neg_terms]

    # Remove bigrams and trigrams of just stopwords or numbers
    #**********************************************************
    from nltk import corpus
    ENGLISH_STOPWORDS = corpus.stopwords.words('english')
    count = 0
    bigrams = top_bigrams
    top_bigrams = []
    for phrase in bigrams:
      words = phrase.split(" ")
      if (((words[0] not in ENGLISH_STOPWORDS) and (not words[0].isdigit())) or ((words[1] not in ENGLISH_STOPWORDS) and (not words[1].isdigit()))) and count <= opt_maxNumberOfTerms:
        count = count + 1
        top_bigrams.append(phrase)


    count = 0
    trigrams = top_trigrams
    top_trigrams = []
    for phrase in trigrams:
      words = phrase.split(" ")
      if (((words[0] not in ENGLISH_STOPWORDS) and (not words[0].isdigit())) or ((words[1] not in ENGLISH_STOPWORDS) and (not words[1].isdigit()))) and count <= opt_maxNumberOfTerms:
        count = count + 1
        top_trigrams.append(phrase)
    #**********************************************************

    s_fields = {
      "tag": "Custom",
      "index": es_info['activeDomainIndex'],
      "doc_type": es_info['docType']
    }

    custom_terms = [field['term'][0] for field in multifield_query_search(s_fields, 500, ['term'], self._termsIndex, 'terms', self._es)]

    for term in custom_terms:
      try:
        top_terms = top_terms.remove(term)
      except ValueError:
        continue
      try:
        top_bigrams.remove(term)
      except ValueError:
        continue
      try:
        top_trigrams.remove(term)
      except ValueError:
        continue

    if not top_terms:
      return []

    pos_data = {field['id']:" ".join(field[es_info['mapping']['text']][0].split(" ")[0:MAX_TEXT_LENGTH]) for field in term_search(es_info['mapping']['tag'], ['Relevant'], self._all, ['url', es_info['mapping']['text']], es_info['activeDomainIndex'], es_info['docType'], self._es)}
    pos_urls = pos_data.keys();
    pos_text = pos_data.values();

    total_pos_tf = None
    total_pos = 0

    total_bigram_pos_tf = None
    total_bigram_pos = 0

    total_trigram_pos_tf = None
    total_trigram_pos = 0

    pos_corpus = []
    pos_bigram_corpus = []
    pos_trigram_corpus = []

    if len(pos_urls) > 1:
      [ttfs_pos,pos_corpus,_] = getTermFrequency(pos_urls, pos_tags=self._pos_tags, term_freq=MAX_TERM_FREQ, mapping=es_info['mapping'], es_index=es_info['activeDomainIndex'], es_doc_type=es_info['docType'], es=self._es)

      total_pos_tf = np.sum(ttfs_pos, axis=0)
      total_pos = np.sum(total_pos_tf)

      [_,_,bigram_tf_data,trigram_tf_data,pos_bigram_corpus, pos_trigram_corpus,_,_] = get_bigrams_trigrams.get_bigrams_trigrams(pos_text, opt_maxNumberOfTerms, self._es)


      total_bigram_pos_tf = np.transpose(np.sum(bigram_tf_data, axis=0))
      total_bigram_pos = np.transpose(np.sum(total_bigram_pos_tf))

      total_trigram_pos_tf = trigram_tf_data.sum(axis=0)
      total_trigram_pos = np.sum(total_trigram_pos_tf)

    neg_data = {field['id']:" ".join(field[es_info['mapping']['text']][0].split(" ")[0:MAX_TEXT_LENGTH]) for field in term_search(es_info['mapping']['tag'], ['Irrelevant'], self._all, ['url', es_info['mapping']['text']], es_info['activeDomainIndex'], es_info['docType'], self._es)}
    neg_urls = neg_data.keys();
    neg_text = neg_data.values();

    neg_freq = {}
    total_neg_tf = None
    total_neg = 0

    total_bigram_neg_tf = None
    total_bigram_neg = 0

    total_trigram_neg_tf = None
    total_trigram_neg = 0

    neg_corpus = []
    neg_bigram_corpus = []
    neg_trigram_corpus = []

    if len(neg_urls) > 1:
      [ttfs_neg,neg_corpus,_] = getTermFrequency(neg_urls, pos_tags=self._pos_tags, term_freq=MAX_TERM_FREQ, mapping=es_info['mapping'], es_index=es_info['activeDomainIndex'], es_doc_type=es_info['docType'], es=self._es)

      total_neg_tf = np.sum(ttfs_neg, axis=0)
      total_neg = np.sum(total_neg_tf)

      [_,_,bigram_tf_data,trigram_tf_data,neg_bigram_corpus, neg_trigram_corpus,_,_] = get_bigrams_trigrams.get_bigrams_trigrams(neg_text, opt_maxNumberOfTerms, self._es)

      total_bigram_neg_tf = np.transpose(np.sum(bigram_tf_data, axis=0))
      total_bigram_neg = np.sum(total_bigram_neg_tf)

      total_trigram_neg_tf = np.transpose(trigram_tf_data.sum(axis=0))
      total_trigram_neg = np.sum(total_trigram_neg_tf)

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

    for key in top_bigrams + [term for term in custom_terms if term in pos_bigram_corpus or term in neg_bigram_corpus]:
      if key in pos_bigram_corpus:
        if total_bigram_pos != 0:
          entry[key] = {"pos_freq": (float(total_bigram_pos_tf[pos_bigram_corpus.index(key)])/total_bigram_pos)}
        else:
          entry[key] = {"pos_freq": 0}
      else:
        entry[key] = {"pos_freq": 0}
      if key in neg_bigram_corpus:
        if total_bigram_neg != 0:
          entry[key].update({"neg_freq": (float(total_bigram_neg_tf[neg_bigram_corpus.index(key)])/total_bigram_neg)})
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

    for key in top_trigrams + [term for term in custom_terms if term in pos_trigram_corpus or term in neg_trigram_corpus]:
      if key in pos_trigram_corpus:
        if total_trigram_pos != 0:
          entry[key] = {"pos_freq": (float(total_trigram_pos_tf[0,pos_trigram_corpus.index(key)])/total_trigram_pos)}
        else:
          entry[key] = {"pos_freq": 0}
      else:
        entry[key] = {"pos_freq": 0}
      if key in neg_trigram_corpus:
        if total_trigram_neg != 0:
          entry[key].update({"neg_freq": (float(total_trigram_neg_tf[neg_trigram_corpus.index(key)])/total_trigram_neg)})
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

    terms = [[key, entry[key]["pos_freq"], entry[key]["neg_freq"], entry[key]["tag"]] for key in custom_terms + top_terms + top_bigrams + top_trigrams]

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

    format = '%m/%d/%Y %H:%M %Z'
    if not session.get('fromDate') is None:
      session['fromDate'] = long(DomainModel.convert_to_epoch(datetime.strptime(session['fromDate'], format)))

    if not session.get('toDate') is None:
      session['toDate'] = long(DomainModel.convert_to_epoch(datetime.strptime(session['toDate'], format)))

    hits = self._getPagesQuery(session)

    docs = {}
    for hit in hits:
      doc = {}
      if not hit.get('description') is None:
        doc["snippet"] = " ".join(hit['description'][0].split(" ")[0:20])
      if not hit.get('image_url') is None:
        doc["image_url"] = hit['image_url'][0]
      if not hit.get('title') is None:
        doc["title"] = hit['title'][0]
      if not hit.get(es_info['mapping']['tag']) is None:
        doc["tags"] = hit[es_info['mapping']['tag']]
      if not hit.get("rank") is None:
        doc["tags"] = hit["rank"]
      if not hit.get(es_info['mapping']["timestamp"]) is None:
        doc["timestamp"] = hit[es_info['mapping']["timestamp"]]

      docs[hit['url'][0]] = doc

    return docs

  def _getMostRecentPages(self, session):
    es_info = self._esInfo(session['domainId'])

    hits = []
    if session['fromDate'] is None:
      hits = get_most_recent_documents(session['pagesCap'], es_info['mapping'], ["url", "description", "image_url", "title", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"], es_info['mapping']["text"]],
                                       session['filter'],
                                       es_info['activeDomainIndex'],
                                       es_info['docType'],
                                       self._es)
    else:
      if(session['filter'] is None):
        hits = range_search(es_info['mapping']["timestamp"], session['fromDate'], session['toDate'], ["url", "description", "image_url", "title", "x", "y", es_info['mapping']['tag'], es_info['mapping']["timestamp"], es_info['mapping']["text"]], True, session['pagesCap'],
                            es_info['activeDomainIndex'],
                            es_info['docType'],
                            self._es)
      else:
        s_fields = {
          es_info['mapping']["text"]: "(" + session['filter'].replace('"','\"') + ")",
          es_info['mapping']["timestamp"]: "[" + str(session['fromDate']) + " TO " + str(session['toDate']) + "]"
        }
        hits = multifield_query_search(s_fields, session['pagesCap'], ["url", "description", "image_url", "title", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"], es_info['mapping']["text"]],
                                       es_info['activeDomainIndex'],
                                       es_info['docType'],
                                       self._es)
    return hits

  def _getPagesForQueriesTags(self, session):
    es_info = self._esInfo(session['domainId'])

    s_fields = {}
    s_fields_aux = {}
    if not session['filter'] is None:
      s_fields[es_info['mapping']["text"]] =   session['filter'].replace('"','\"')
      s_fields_aux[es_info['mapping']["text"]] =   session['filter'].replace('"','\"')

    if not session['fromDate'] is None:
      s_fields[es_info['mapping']["timestamp"]] = "[" + str(session['fromDate']) + " TO " + str(session['toDate']) + "]"
      s_fields_aux[es_info['mapping']["timestamp"]] = "[" + str(session['fromDate']) + " TO " + str(session['toDate']) + "]"

    hits=[]
    n_criteries = session['newPageRetrievalCriteria'].split(',')
    for criteria in n_criteries:
        if criteria != "":
            if criteria == "Queries":
                queries = session['selected_queries'].split(',')
            elif criteria == "Tags":
                tags = session['selected_tags'].split(',')

    for query in queries:
        for tag in tags:
            if tag != "":
                if tag == "Neutral":
                    query_aux = "(" + "query" + ":" + query + ")"
                    query_field_missing = {
                        "filtered" : {
                          "query": {
                              "query_string": {
                                  "query": query_aux
                              }
                          },
                          "filter" : {
                            "missing" : { "field" : "tag" }
                          }
                        }
                    }

                    s_fields_aux["queries"] = [query_field_missing]

                    results = multifield_term_search(s_fields_aux, session['pagesCap'], ["url", "description", "image_url", "title", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"], es_info['mapping']["text"]],
                                            es_info['activeDomainIndex'],
                                            es_info['docType'],
                                            self._es)

                else:
                    s_fields[es_info['mapping']['tag']] = '"' + tag + '"'
                    s_fields[es_info['mapping']["query"]] = '"' + query + '"'
                    results= multifield_query_search(s_fields, session['pagesCap'], ["url", "description", "image_url", "title", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"], es_info['mapping']["text"]],
                                            es_info['activeDomainIndex'],
                                            es_info['docType'],
                                            self._es)
                if session['selected_morelike']=="moreLike":
                    aux_result = self._getMoreLikePagesAll(session, results)
                    hits.extend(aux_result)
                else:
                    hits.extend(results)


    return hits


  def _getPagesForMultiCriteria(self, session):
    es_info = self._esInfo(session['domainId'])

    print "\n\nMULTI CRITERIA\n"
    
    s_fields = {}
    if not session['filter'] is None:
      s_fields[es_info['mapping']["text"]] =   session['filter'].replace('"','\"')

    if not session['fromDate'] is None:
      s_fields[es_info['mapping']["timestamp"]] = "[" + str(session['fromDate']) + " TO " + str(session['toDate']) + "]"

    hits=[]
    n_criteria = session['pageRetrievalCriteria'].keys()
    n_criteria_vals = [val.split(",") for val in session['pageRetrievalCriteria'].values()]

    print "\n\n\n  n_criteria_vals",  n_criteria_vals,"\n\n\n"

    criteria_comb = product(*[range(0,len(val)) for val in n_criteria_vals])

    print "\n\n\n  criteria_comb",  criteria_comb,"\n\n\n"
    
    for criteria in criteria_comb:
      print "\n\n\n  criteria",  criteria,"\n\n\n"
      i = 0
      for criterion_index in criteria:
        criterion = n_criteria_vals[i][criterion_index]
        print "\n\n\n  criterion",criterion,"\n\n\n"
        n_criterion = n_criteria[i]
        if n_criterion == 'tag' and criterion == "Neutral":
          if s_fields.get("tag"):
            s_fields.pop("tag")
          s_fields["filter"] = {
            "missing" : { "field" : "tag" }
          }
        elif(n_criterion in 'Maybe relevant'):
            s_fields["label_neg"] =  1
        elif(n_criterion in 'Maybe irrelevant'):
            s_fields["label_pos"] =  1
        elif(n_criterion in 'Unsure'):
            s_fields["unsure_tag"] =  1
        else:
          s_fields[n_criterion] =  criterion 
        i = i+1
      results= multifield_term_search(s_fields, session['pagesCap'], ["url", "description", "image_url", "title", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"], es_info['mapping']["text"]],
                                      es_info['activeDomainIndex'],
                                      es_info['docType'],
                                      self._es)
      if s_fields.get("filter") is not None:
        s_fields.pop("filter")
      if session['selected_morelike']=="moreLike":
        morelike_result = self._getMoreLikePagesAll(session, results)
        hits.extend(morelike_result)
      else:
        hits.extend(results)
            
    return hits

  
  def _getPagesForQueries(self, session):
    es_info = self._esInfo(session['domainId'])

    s_fields = {}
    if not session['filter'] is None:
      s_fields[es_info['mapping']["text"]] =   session['filter'].replace('"','\"')

    if not session['fromDate'] is None:
      s_fields[es_info['mapping']["timestamp"]] = "[" + str(session['fromDate']) + " TO " + str(session['toDate']) + "]"

    hits=[]
    queries = session['selected_queries'].split(',')

    for query in queries:
      s_fields[es_info['mapping']["query"]] = '"' + query + '"'
      results= multifield_query_search(s_fields,
                                       session['pagesCap'],
                                       ["url", "description", "image_url", "title", "rank", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"]],
                                       es_info['activeDomainIndex'],
                                       es_info['docType'],
                                       self._es)
      if session['selected_morelike']=="moreLike":
        aux_result = self._getMoreLikePagesAll(session, results)
        hits.extend(aux_result)
      else:
        hits.extend(results)
    return hits

  def _getPagesForTLDs(self, session):
    es_info = self._esInfo(session['domainId'])

    s_fields = {}
    if not session['filter'] is None:
      s_fields[es_info['mapping']["text"]] =   session['filter'].replace('"','\"')

    if not session['fromDate'] is None:
      s_fields[es_info['mapping']["timestamp"]] = "[" + str(session['fromDate']) + " TO " + str(session['toDate']) + "]"

    hits=[]
    tlds = session['selected_tlds'].split(',')

    for tld in tlds:
      s_fields[es_info['mapping']['domain']] = '"' + tld + '"'
      results= multifield_query_search(s_fields,
                                       session['pagesCap'],
                                       ["url", "description", "image_url", "title", "rank", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"]],
                                       es_info['activeDomainIndex'],
                                       es_info['docType'],
                                       self._es)
      if session['selected_morelike']=="moreLike":
        aux_result = self._getMoreLikePagesAll(session, results)
        hits.extend(aux_result)
      else:
        hits.extend(results)
    return hits
  
  def _getPagesForTags(self, session):
    es_info = self._esInfo(session['domainId'])

    s_fields = {}
    if not session['filter'] is None:
      s_fields[es_info['mapping']["text"]] = session['filter'].replace('"','\"')

    if not session['fromDate'] is None:
      s_fields[es_info['mapping']["timestamp"]] = "[" + str(session['fromDate']) + " TO " + str(session['toDate']) + "]"

    hits=[]
    tags = session['selected_tags'].split(',')
    for tag in tags:
      if tag != "":
        if tag == "Neutral":
          s_fields["filter"] = {
            "missing" : { "field" : "tag" }
          }
          
          results = multifield_term_search(s_fields, session['pagesCap'], ["url", "description", "image_url", "title", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"], es_info['mapping']["text"]],
                                           es_info['activeDomainIndex'],
                                           es_info['docType'],
                                           self._es)

          if session['selected_morelike']=="moreLike":
            aux_result = self._getMoreLikePagesAll(session, results)
            hits.extend(aux_result)
          else:
            hits.extend(results)
          s_fields.pop("filter")
          
          s_fields["tag"] = ""

          results = multifield_term_search(s_fields, session['pagesCap'], ["url", "description", "image_url", "title", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"], es_info['mapping']["text"]],
                                           es_info['activeDomainIndex'],
                                           es_info['docType'],
                                           self._es)

          if session['selected_morelike']=="moreLike":
              aux_result = self._getMoreLikePagesAll(session, results)
              hits.extend(aux_result)
          else:
              hits.extend(results)
          s_fields.pop("tag")
        else:
          #Added a wildcard query as tag is not analyzed field
          query = {
            "wildcard": {es_info['mapping']["tag"]:"*" + tag + "*"}
          }
          s_fields["queries"] = [query]
          results= multifield_term_search(s_fields, session['pagesCap'], ["url", "description", "image_url", "title", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"], es_info['mapping']["text"]],
                                          es_info['activeDomainIndex'],
                                          es_info['docType'],
                                          self._es)
          if session['selected_morelike']=="moreLike":
              aux_result = self._getMoreLikePagesAll(session, results)
              hits.extend(aux_result)
          else:
              hits.extend(results)
          s_fields.pop("queries");    
    return hits

  def _getRelevantPages(self, session):
    es_info = self._esInfo(session['domainId'])

    pos_hits = search(es_info['mapping']['tag'], ['relevant'], session['pagesCap'], ["url", "description", "image_url", "title", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"], es_info['mapping']["text"]], es_info['activeDomainIndex'], 'page', self._es)

    return pos_hits

  def _getMoreLikePages(self, session):
    es_info = self._esInfo(session['domainId'])

    hits=[]
    tags = session['selected_tags'].split(',')
    for tag in tags:
      tag_hits = search(es_info['mapping']['tag'], [tag], session['pagesCap'], ["url", "description", "image_url", "title", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"], es_info['mapping']["text"]], es_info['activeDomainIndex'], 'page', self._es)

      if len(tag_hits) > 0:
        tag_urls = [field['id'] for field in tag_hits]

        results = get_more_like_this(tag_urls, ["url", "description", "image_url", "title", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"], es_info['mapping']["text"]], session['pagesCap'],  es_info['activeDomainIndex'], es_info['docType'],  self._es)

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
    unsure_label_hits = term_search("unsure_tag", "1", MAX_LABEL_PAGES, ["url", "description", "image_url", "title", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"], es_info['mapping']["text"]], es_info['activeDomainIndex'], es_info['docType'], self._es)

    return unsure_label_hits

  def _getPosLabelPages(self, session):
    es_info = self._esInfo(session['domainId'])

    pos_label_hits = term_search("label_pos", "1", MAX_LABEL_PAGES, ["url", "description", "image_url", "title", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"], es_info['mapping']["text"]], es_info['activeDomainIndex'], es_info['docType'], self._es)

    return pos_label_hits

  def _getNegLabelPages(self, session):
    es_info = self._esInfo(session['domainId'])

    neg_label_hits = term_search("label_neg", "1", MAX_LABEL_PAGES, ["url", "description", "image_url", "title", "x", "y", es_info['mapping']["tag"], es_info['mapping']["timestamp"], es_info['mapping']["text"]], es_info['activeDomainIndex'], es_info['docType'], self._es)

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
      if session['selected_model_tags'] == 'Unsure':
        hits = self._getUnsureLabelPages(session)
      elif (session.get('selected_model_tags') == 'Maybe relevant'):
        hits = self._getPosLabelPages(session)
      elif (session.get('selected_model_tags') == 'Maybe irrelevant'):
        hits = self._getNegLabelPages(session)

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
      pos_docs = pos_docs + multifield_term_search(s_fields, self._all, ["url", es_info['mapping']['html']],
                                                   es_info['activeDomainIndex'],
                                                   es_info['docType'],
                                                   self._es)
    neg_docs = []
    for tag in neg_tags.split(','):
      s_fields = {}
      query = {
        "wildcard": {es_info['mapping']["tag"]:tag}
      }
      s_fields["queries"] = [query]
      neg_docs = neg_docs + multifield_term_search(s_fields, self._all, ["url", es_info['mapping']['html']],
                                                   es_info['activeDomainIndex'],
                                                   es_info['docType'],
                                                   self._es)

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
    comm = ache_home + "/bin/ache buildModel -t " + data_training + " -o "+ domainmodel_dir + " -c " + ache_home + "/config/stoplist.txt"
    p = Popen(comm, shell=True, stderr=PIPE)
    output, errors = p.communicate()
    print output
    print errors

    if zip:
      zip_filename = data_domain + es_info['activeDomainIndex'] + "_model.zip"
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
    es_info = self._esInfo(session['domainId'])

    onlineClassifier = None
    trainedPosSamples = []
    trainedNegSamples = []
    onlineClassifierInfo = self._onlineClassifiers.get(session['domainId'])
    if onlineClassifierInfo == None:
      onlineClassifier = OnlineClassifier()
      self._onlineClassifiers[session['domainId']] = {"onlineClassifier":onlineClassifier}
      self._onlineClassifiers[session['domainId']]["trainedPosSamples"] = []
      self._onlineClassifiers[session['domainId']]["trainedNegSamples"] = []
    else:
      onlineClassifier = self._onlineClassifiers[session['domainId']]["onlineClassifier"]
      trainedPosSamples = self._onlineClassifiers[session['domainId']]["trainedPosSamples"]
      trainedNegSamples = self._onlineClassifiers[session['domainId']]["trainedNegSamples"]

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

    pos_docs = exec_query(query, ["url", es_info['mapping']['text']], self._all,
                          es_info['activeDomainIndex'],
                          es_info['docType'],
                          self._es)

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
    neg_docs = exec_query(query, ["url", es_info['mapping']['text']], self._all,
                          es_info['activeDomainIndex'],
                          es_info['docType'],
                          self._es)
    neg_text = [neg_doc[es_info['mapping']['text']][0][0:MAX_TEXT_LENGTH] for neg_doc in neg_docs]
    neg_ids = [neg_doc["id"] for neg_doc in neg_docs]
    neg_labels = [0 for i in range(0, len(neg_text))]

    print "\n\n\nNew relevant samples ", len(pos_text),"\n", "New irrelevant samples ",len(neg_text), "\n\n\n"

    clf = None
    train_data = None
    if pos_text or neg_text:
      self._onlineClassifiers[session['domainId']]["trainedPosSamples"] = self._onlineClassifiers[session['domainId']]["trainedPosSamples"] + pos_ids
      self._onlineClassifiers[session['domainId']]["trainedNegSamples"] = self._onlineClassifiers[session['domainId']]["trainedNegSamples"] + neg_ids
      [train_data,_] = self._onlineClassifiers[session['domainId']]["onlineClassifier"].vectorize(pos_text+neg_text)
      self._onlineClassifiers[session['domainId']]["onlineClassifier"].partialFit(train_data, pos_labels+neg_labels)

    # ****************************************************************************************

    # Fit calibratrated classifier


    if train_data != None:

      trainedPosSamples = self._onlineClassifiers[session['domainId']]["trainedPosSamples"]
      trainedNegSamples = self._onlineClassifiers[session['domainId']]["trainedNegSamples"]
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
        [calibrate_pos_data,_] = self._onlineClassifiers[session['domainId']]["onlineClassifier"].vectorize(pos_trained_text)
        [calibrate_neg_data,_] = self._onlineClassifiers[session['domainId']]["onlineClassifier"].vectorize(neg_trained_text)
        calibrate_pos_labels = pos_trained_labels
        calibrate_neg_labels = neg_trained_labels

        calibrate_data = sp.sparse.vstack([calibrate_pos_data, calibrate_neg_data]).toarray()
        calibrate_labels = calibrate_pos_labels+calibrate_neg_labels

        train_indices = np.random.choice(len(calibrate_labels), 2*len(calibrate_labels)/3)
        test_indices = np.random.choice(len(calibrate_labels), len(calibrate_labels)/3)

        sigmoid = onlineClassifier.calibrate(calibrate_data[train_indices], np.asarray(calibrate_labels)[train_indices])
        if not sigmoid is None:
          self._onlineClassifiers[session['domainId']]["sigmoid"] = sigmoid
          accuracy = round(self._onlineClassifiers[session['domainId']]["onlineClassifier"].calibrateScore(sigmoid, calibrate_data[test_indices], np.asarray(calibrate_labels)[test_indices]), 4) * 100
          self._onlineClassifiers[session['domainId']]["accuracy"] = str(accuracy)

          print "\n\n\n Accuracy = ", accuracy, "%\n\n\n"
        else:
          print "\n\n\nNot enough data for calibration\n\n\n"
      else:
        print "\n\n\nNot enough data for calibration\n\n\n"

      # ****************************************************************************************


    accuracy = '0'
    if self._onlineClassifiers.get(session['domainId']) != None:
      accuracy = self._onlineClassifiers[session['domainId']].get("accuracy")
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

    sigmoid = self._onlineClassifiers[session['domainId']].get("sigmoid")
    if sigmoid != None:
      unlabelled_docs = field_missing(es_info["mapping"]["tag"], ["url", es_info["mapping"]["text"]], self._all,
                                      es_info['activeDomainIndex'],
                                      es_info['docType'],
                                      self._es)

      if len(unlabelled_docs) > 2000:
        unlabelled_docs = sample(unlabelled_docs, 2000)

      unlabeled_text = [unlabelled_doc[es_info['mapping']['text']][0][0:MAX_TEXT_LENGTH] for unlabelled_doc in unlabelled_docs]

      # Check if unlabeled data available
      if len(unlabeled_text) > 0:
        unlabeled_urls = [unlabelled_doc[es_info['mapping']['url']][0] for unlabelled_doc in unlabelled_docs]
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

