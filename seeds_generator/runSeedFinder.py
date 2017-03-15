import urllib
from subprocess import call
from subprocess import Popen
from subprocess import PIPE

from os import chdir, listdir, environ, makedirs, rename, chmod, walk, devnull, remove

from os.path import isfile, join, exists, isdir

# Return the seed urls generated for the given query
def collect_seed_urls(query, seed_dir, es_info):
    encoded_query = urllib.quote(query).replace("%5C","")
    with open(seed_dir+"seeds_"+"+".join(encoded_query.split("%20"))+".txt","r") as f:
        return [query, " ".join([url.strip() for url in f.readlines()]), es_info]
    
def execSeedFinder(terms, data_path, es_info):
    domain_name = es_info['activeDomainIndex']
  
    data_dir = data_path + "/data/"
    data_crawler  = data_dir + domain_name
    data_training = data_crawler + "/training_data/"
    
    crawlermodel_dir = data_crawler + "/models/"

    if (not isdir(crawlermodel_dir)):
      return

    seed_dir = data_crawler + "/seedFinder/"
    
    if (not isdir(seed_dir)):
      # Create dir if it does not exist
      makedirs(seed_dir)
    if (not isdir(seed_dir+"log")):
        makedirs(seed_dir+"log")
        
    ache_home = environ['ACHE_HOME']

    comm = ache_home + "/bin/ache run focusedCrawler.seedfinder.SeedFinder --initialQuery \"" +terms + "\" --modelPath " + crawlermodel_dir + " --seedsPath " + seed_dir + " --maxPages 2 --maxQueries 10"
    encoded_query = urllib.quote(terms).replace("%5C","")
    f_sf_log = open(seed_dir+"log/seeds_"+"+".join(encoded_query.split("%20"))+".log", 'w')
    p = Popen(comm, shell=True, stderr=PIPE, stdout=f_sf_log)
    p.wait()

    return collect_seed_urls(terms, seed_dir, es_info)

