# import bokeh.embed
# import bokeh.resources
import cherrypy
from ConfigParser import ConfigParser
import json
import os
from threading import Lock
import urlparse

from functools32 import lru_cache
from jinja2 import Template, Environment, FileSystemLoader

env = Environment(loader=FileSystemLoader(os.path.join(os.path.dirname(__file__), 'html/')))
cherrypy.engine.timeout_monitor.unsubscribe()

class Page(object):
  @staticmethod
  def getConfig(path):
    # Parses file to prevent cherrypy from restarting when config.conf changes: after each request
    # it restarts saying config.conf changed, when it did not.
    config = ConfigParser()
    config.read(os.path.join(path, "config.conf"))

    configMap = {}
    for section in config.sections():
      configMap[section] = {}
      for option in config.options(section):
        # Handles specific integer entries.
        val = config.get(section, option)
        if option == "server.socket_port" or option == "server.thread_pool":
          val = int(val)
        configMap[section][option] = val

    return configMap

  # Extracts boolean parameter.
  @staticmethod
  def extractBooleanParam(param):
    return param == 'true'

  # Extracts list of parameters: array is encoded as a long string with a delimiter.
  @staticmethod
  def extractListParam(param, opt_char = None):
    delimiter = opt_char if opt_char != None else '|'
    return param.split(delimiter) if len(param) > 0 else []

  # Default constructor reading app config file.
  def __init__(self, models, path):
    # Folder with html content.
    self._HTML_DIR = os.path.join(path, u"../client/build")
    self.lock = Lock()
    # TODO Use SeedCrawlerModelAdapter self._domain_model = SeedCrawlerModelAdapter()
    self._domain_model = models["domain"]
    self._crawler_model = models["crawler"]


  @cherrypy.expose
  def getStatus(self, session):
    session = json.loads(session)
    res = self._domain_model.getStatus(session)
    cherrypy.response.headers["Content-Type"] = "application/json;"
    return json.dumps(res)

  # Returns a list of available crawlers in the format:
  # [
  #   {'id': crawlerId, 'name': crawlerName, 'creation': epochInSecondsOfFirstDownloadedURL},
  #   {'id': crawlerId, 'name': crawlerName, 'creation': epochInSecondsOfFirstDownloadedURL},
  #   ...
  # ]
  @cherrypy.expose
  def getAvailableDomains(self, type):
    res = self._domain_model.getAvailableDomains()
    cherrypy.response.headers["Content-Type"] = "application/json;"
    return json.dumps({"crawlers":res, "type":type})

  @cherrypy.expose
  def getAvailableProjectionAlgorithms(self):
    res = self._domain_model.getAvailableProjectionAlgorithms()
    cherrypy.response.headers["Content-Type"] = "application/json;"
    return json.dumps(res)

  @cherrypy.expose
  def getAvailableTLDs(self, session):
    session = json.loads(session)
    res = self._domain_model.getAvailableTLDs(session)
    cherrypy.response.headers["Content-Type"] = "application/json;"
    return json.dumps(res)

  @cherrypy.expose
  def getAvailableQueries(self, session):
    session = json.loads(session)
    res = self._domain_model.getAvailableQueries(session)
    cherrypy.response.headers["Content-Type"] = "application/json;"
    return json.dumps(res)

  @cherrypy.expose
  def getAvailableTags(self, session, event):
    session = json.loads(session)
    res = self._domain_model.getAvailableTags(session)
    result = {
      'tags': res,
      'event':event
    }
    cherrypy.response.headers["Content-Type"] = "application/json;"
    return json.dumps(result)

  @cherrypy.expose
  def getAvailableModelTags(self, session):
    session = json.loads(session)
    result = self._domain_model.getAvailableModelTags(session)
    cherrypy.response.headers["Content-Type"] = "application/json;"
    return json.dumps(result)

  @cherrypy.expose
  def getAvailableCrawledData(self, session):
    session = json.loads(session)
    result = self._domain_model.getAvailableCrawledData(session)
    cherrypy.response.headers["Content-Type"] = "application/json;"
    return json.dumps(result)

  @cherrypy.expose
  def getAnnotatedTerms(self, session):
    session = json.loads(session)
    result = self._domain_model.getAnnotatedTerms(session)
    cherrypy.response.headers["Content-Type"] = "application/json;"
    return json.dumps(result)


  @cherrypy.expose
  def getTagColors(self, domainId):
    res = self._domain_model.getTagColors(domainId)
    cherrypy.response.headers["Content-Type"] = "application/json;"
    return json.dumps(res)

  @cherrypy.expose
  def getModelTags(self, domainId):
    res = self._crawler_model.getModelTags(domainId)
    cherrypy.response.headers["Content-Type"] = "application/json;"
    return json.dumps(res)

  @cherrypy.expose
  def saveModelTags(self, session):
    session = json.loads(session)
    res = self._crawler_model.saveModelTags(session)
    cherrypy.response.headers["Content-Type"] = "application/json;"
    return json.dumps(res)

  @cherrypy.expose
  def updateColors(self, session, colors):
    session = json.loads(session)
    colors = json.loads(colors)
    res = self._domain_model.updateColors(session, colors)
    cherrypy.response.headers["Content-Type"] = "application/json;"
    return json.dumps(res)

  # Submits a web query for a list of terms, e.g. 'ebola disease'
  @cherrypy.expose
  def queryWeb(self, terms, session):
    session = json.loads(session)
    res = self._domain_model.queryWeb(terms, session=session)
    cherrypy.response.headers["Content-Type"] = "text/plain;"
    return res

  # Submits a query for a list of terms, e.g. 'ebola disease' to the seedfinder
  @cherrypy.expose
  def runSeedFinder(self, terms, session):
    session = json.loads(session)
    res = self._domain_model.runSeedFinder(terms, session)
    cherrypy.response.headers["Content-Type"] = "application/json;"
    return json.dumps(res)

  # Add crawler
  @cherrypy.expose
  def addDomain(self, index_name):
    self._domain_model.addDomain(index_name)

  # Delete crawler
  @cherrypy.expose
  def delDomain(self, domains):
    self._domain_model.delDomain(json.loads(domains))


  # Stop Process
  @cherrypy.expose
  def stopProcess(self, process, process_info):
    process_info = json.loads(process_info)
    cherrypy.response.headers["Content-Type"] = "text/plain;"
    return self._domain_model.stopProcess(process, process_info)

  # Returns number of pages downloaded between ts1 and ts2 for active crawler.
  # ts1 and ts2 are Unix epochs (seconds after 1970).
  # If opt_applyFilter is True, the summary returned corresponds to the applied pages filter defined
  # previously in @applyFilter. Otherwise the returned summary corresponds to the entire dataset
  # between ts1 and ts2.
  #
  # For crawler vis, returns dictionary in the format:
  # {
  #   'Positive': {'Explored': #ExploredPgs, 'Exploited': #ExploitedPgs, 'Boosted': #BoostedPgs},
  #   'Negative': {'Explored': #ExploredPgs, 'Exploited': #ExploitedPgs, 'Boosted': #BoostedPgs},
  # }
  #
  # For seed crawler vis, returns dictionary in the format:
  # {
  #   'Relevant': numPositivePages,
  #   'Irrelevant': numNegativePages,
  #   'Neutral': numNeutralPages,
  # }
  @cherrypy.expose
  def getPagesSummary(self, opt_ts1=None, opt_ts2=None, opt_applyFilter=False, session=None):
    session = json.loads(session)
    res = self._domain_model.getPagesSummary(opt_ts1, opt_ts2, opt_applyFilter, session)
    cherrypy.response.headers["Content-Type"] = "application/json;"
    return json.dumps(res)



  # Returns number of terms present in positive and negative pages.
  # Returns array in the format:
  # [
  #   [term, frequencyInPositivePages, frequencyInNegativePages],
  #   [term, frequencyInPositivePages, frequencyInNegativePages],
  #   ...
  # ]
  @cherrypy.expose
  def getTermsSummary(self, session):
    session = json.loads(session)
    res = self._domain_model.getTermsSummaryDomain(session = session)
    cherrypy.response.headers["Content-Type"] = "application/json;"
    return json.dumps(res)

  # Sets limit to pages returned by @getPages.
  @cherrypy.expose
  def setPagesCountCap(self, pagesCap):
    self._domain_model.setPagesCountCap(pagesCap)

  # Set the date range to filter data
  @cherrypy.expose
  def setDateTime(self, fromDate=None, toDate=None):
    self._domain_model.setDateTime(fromDate, toDate)

  # Returns most recent downloaded pages.
  # Returns dictionary in the format:
  # {
  #   'last_downloaded_url_epoch': 1432310403 (in seconds)
  #   'pages': [
  #             [url1, x, y, tags],     (tags are a list, potentially empty)
  #             [url2, x, y, tags],
  #             [url3, x, y, tags],
  #   ]
  # }
  @cherrypy.expose
  def getPages(self, session):
    session = json.loads(session)
    data = self._domain_model.getPages(session)
    colors = self._domain_model.getTagColors(session['domainId'])
    res = {"data": data}#//, "plot": selection_plot(data, colors)}
    cherrypy.response.headers["Content-Type"] = "application/json;"
    return json.dumps(res)

  # Boosts set of pages: crawler exploits outlinks for the given set of pages.
  @cherrypy.expose
  def boostPages(self, pages):
    self._domain_model.boostPages(pages)

  # Crawl forward links
  @cherrypy.expose
  def getForwardLinks(self, urls, session):
    session = json.loads(session)
    self._domain_model.getForwardLinks(urls, session);

  # Crawl backward links
  @cherrypy.expose
  def getBackwardLinks(self, urls, session):
    session = json.loads(session)
    self._domain_model.getBackwardLinks(urls, session);

  # Fetches snippets for a given term.
  @cherrypy.expose
  def getTermSnippets(self, term, session):
    session = json.loads(session)
    res = self._domain_model.getTermSnippets(term, session)
    cherrypy.response.headers["Content-Type"] = "application/json;"
    return json.dumps(res)


  # Adds tag to pages (if applyTagFlag is True) or removes tag from pages (if applyTagFlag is
  # False).
  @cherrypy.expose
  def setPagesTag(self, pages, tag, applyTagFlag, session):
    session = json.loads(session)
    pages = Page.extractListParam(pages)
    applyTagFlag = Page.extractBooleanParam(applyTagFlag)
    self.lock.acquire()
    output = self._domain_model.setPagesTag(pages, tag, applyTagFlag, session)
    self.lock.release()
    return output


  # Adds tag to terms (if applyTagFlag is True) or removes tag from terms (if applyTagFlag is
  # False).
  @cherrypy.expose
  def setTermsTag(self, terms, tag, applyTagFlag, session):
    session = json.loads(session)
    terms = Page.extractListParam(terms)
    applyTagFlag =  Page.extractBooleanParam(applyTagFlag)
    self._domain_model.setTermsTag(terms, tag, applyTagFlag, session)

  # Update online classifier
  @cherrypy.expose
  def updateOnlineClassifier(self, session):
    session = json.loads(session)
    return self._domain_model.updateOnlineClassifier(session)

  # Update unlabeled sample predictions
  @cherrypy.expose
  def predictUnlabeled(self, session):
    session = json.loads(session)
    return self._domain_model.predictUnlabeled(session)


  # Delete terms from term window and from the ddt_terms index
  @cherrypy.expose
  def deleteTerm(self, term, session):
    session = json.loads(session)
    self._domain_model.deleteTerm(term, session)

  # Download the pages of uploaded urls
  @cherrypy.expose
  def uploadUrls(self, urls, tag, session):
    urls = urls.replace("\n", " ")
    session = json.loads(session)
    res = self._domain_model.uploadUrls(urls, tag, session)
    cherrypy.response.headers["Content-Type"] = "application/json;"
    return json.dumps({"status": "Done"})

  # Extracts terms with current labels state.
  @cherrypy.expose
  def extractTerms(self, numberOfTerms, session):
    session = json.loads(session)
    numberOfTerms = int(numberOfTerms)
    res = self._domain_model.extractTerms(numberOfTerms, session)

    cherrypy.response.headers["Content-Type"] = "application/json;"
    return json.dumps(res)

  # Create model
  @cherrypy.expose
  def createModel(self, session):
    session = json.loads(session)
    esInfo = self._domain_model.esInfo(session["domainId"])
    return self._crawler_model.createModel(session)

  # Run Crawler
  @cherrypy.expose
  def startCrawler(self, session, type="focused", seeds=""):
    session = json.loads(session)
    seeds = self.extractListParam(seeds)
    cherrypy.response.headers["Content-Type"] = "text/plain;"
    return self._crawler_model.startCrawler(type, seeds , session)

  # Stop Crawler
  @cherrypy.expose
  def stopCrawler(self, session, type="focused"):
    session = json.loads(session)
    cherrypy.response.headers["Content-Type"] = "text/plain;"
    return self._crawler_model.stopCrawler(type, session)

  # Get recommendations for deep crawling
  @cherrypy.expose
  def getRecommendations(self, session):
    session = json.loads(session)
    res = self._crawler_model.getRecommendations(session)
    cherrypy.response.headers["Content-Type"] = "text/json;"
    return json.dumps(res)

  # Returns available dataset options.
  @cherrypy.expose
  def getAvailableDatasets(self):
    cherrypy.response.headers["Content-Type"] = "application/json;"
    return json.dumps(TrainSetDataLoader._DATASET_OPTIONS.keys())


  # Given dataset name, returns json with term-index and topic-term distributions for +/- examples
  # in training set.
  @cherrypy.expose
  def getTrainingSetTopics(self, datasetName):
    # Data for positive page examples.
    pos = True
    posData = { \
    "topic-term": TrainSetDataLoader.getTopicTermData(datasetName, pos), \
    "topic-cosdistance": TrainSetDataLoader.getCosDistanceData(datasetName, pos), \
    "pca": TrainSetDataLoader.getPCAData(datasetName, pos), \
    "term-index": TrainSetDataLoader.getTermIndexData(datasetName, pos)}

    # Data for negative page examples.
    pos = False
    negData = { \
    "topic-term": TrainSetDataLoader.getTopicTermData(datasetName, pos), \
    "topic-cosdistance": TrainSetDataLoader.getCosDistanceData(datasetName, pos), \
    "pca": TrainSetDataLoader.getPCAData(datasetName, pos), \
    "term-index": TrainSetDataLoader.getTermIndexData(datasetName, pos)}

    # Returns object for positive and negative page examples.
    cherrypy.response.headers["Content-Type"] = "application/json;"
    return json.dumps({"positive": posData, "negative": negData})

  # @cherrypy.expose
  # def getEmptyBokehPlot(self):
  #   cherrypy.response.headers["Content-Type"] = "application/json;"
  #   return json.dumps(empty_plot())

  @lru_cache(maxsize=5)
  def make_pages_query(self, session):
    session = json.loads(session)
    pages = self._domain_model.getPlottingData(session)
    return parse_es_response(pages)

  @cherrypy.expose
  def statistics(self, session):
    df = self.make_pages_query(session)
    plots_script, plots_div = create_plot_components(df, top_n=10)
    widgets_script, widgets_div = create_table_components(df)

    template = env.get_template('cross_filter.html')

    return template.render(plots_script=plots_script,
                           plots_div=plots_div,
                           widgets_script=widgets_script,
                           widgets_div=widgets_div,
    )

  @cherrypy.expose
  @cherrypy.tools.json_out()
  @cherrypy.tools.json_in()
  def update_cross_filter_plots(self, session):
    df = self.make_pages_query(session)

    state = cherrypy.request.json

    # limit number of hostnames to 10
    top_n = 10

    ## applying the 'filter' of the cross_filter
    if state['urls']:
        df = df[df.hostname.isin(state['urls'])]
        top_n = None
    if state['tlds']:
        df = df[df.tld.isin(state['tlds'])]
    if state['tags']:
        df = df[df.tag.isin(state['tags'])]
    if state['queries']:
        df = df[df['query'].isin(state['queries'])]
    if state['datetimepicker_start']:
        df = df[state['datetimepicker_start']:]
    if state['datetimepicker_end']:
        df = df[:state['datetimepicker_end']]

    plots_script, plots_div = create_plot_components(df, top_n=top_n)

    template = env.get_template('cross_filter_plot_area.html')

    return template.render(plots_script=plots_script,
                           plots_div=plots_div,
                           )

# if __name__ == "__main__":
#   from models import domain_discovery_model
#   path = os.path.dirname(__file__)
#   model = domain_discovery_model.DomainModel()
#   page = Page(model, path)

#   # CherryPy always starts with app.root when trying to map request URIs
#   # to objects, so we need to mount a request handler root. A request
#   # to "/" will be mapped to HelloWorld().index().
#   app = cherrypy.quickstart(page, config=Page.getConfig(path))
#   cherrypy.config.update(page.config)
#   #app = cherrypy.tree.mount(page, "/", page.config)

#   #if hasattr(cherrypy.engine, "signal_handler"):
#   #    cherrypy.engine.signal_handler.subscribe()
#   #if hasattr(cherrypy.engine, "console_control_handler"):
#   #    cherrypy.engine.console_control_handler.subscribe()
#   #cherrypy.engine.start()
#   #cherrypy.engine.block()

# else:
#   page = Page()
#   # This branch is for the test suite; you can ignore it.
#   config = Page.getConfig()
#   app = cherrypy.tree.mount(page, config=config)
