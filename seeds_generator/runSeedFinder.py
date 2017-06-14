import urllib
from time import sleep

from subprocess import call
from subprocess import Popen
from subprocess import PIPE

from seeds_generator.download import callDownloadUrls

from os import chdir, listdir, environ, makedirs, rename, chmod, walk, devnull, remove

from os.path import isfile, join, exists, isdir

class CollectSeeds:
    """Collect seeds generated by SeedFinder and start downloading them into elasticsearch
    """
    query = None
    csv_file = None
    es_info = None
    
    def __init__(self, query, csv_file, es_info):
        self.query = query
        self.csv_file = csv_file
        self.es_info = es_info
        
    def collect_seed_urls(self, callback, p):
        print "\n\n\n COLLECT SEED URLS ",self.query," ", self.csv_file

        curr_subquery = None
        urls = []
        # Wait for the self.csv_file to be created
        while True:
            try:
                if isfile(self.csv_file):
                    break
                sleep(5)
            except IOError, message:
                print "File is locked (unable to open in append mode). %s.", message
                
        f = open(self.csv_file, "r")

        count = 0
        # Process till the SeedFinder subprocess is not done
        while p.poll() is None:
            line = f.readline()
            
            if line is None or line.strip() == "":
                sleep(2)
                continue

            if line.strip().split(",")[1].strip() == "relevant":
                items = line.strip().split(',')
                subquery = urllib.unquote(items[0].strip()).replace("+"," ")
                url = urllib.unquote(items[2].strip())
                
                if subquery == curr_subquery:
                    urls.append(url)
                else:
                    if urls:
                        count = count + len(urls)
                        # Inform that the pages have started downloading
                        callback(self.query, "Downloading")
                        
                        # Download urls for a specific subquery
                        callDownloadUrls("seedfinder:"+self.query, curr_subquery, " ".join(urls), self.es_info)
                    curr_subquery = subquery
                    urls = [url]
        if urls:
            count = count + len(urls)
            # Inform that the pages have started downloading
            callback(self.query, "Downloading")
            
            # Download urls for a specific subquery
            callDownloadUrls("seedfinder:"+self.query, curr_subquery, " ".join(urls), self.es_info)

        if count == 0:
            callback(self.query, "No relevant results")
        else:
            callback(self.query, "Completed")
                    
class RunSeedFinder:

    def execSeedFinder(self, terms, data_path, callback, es_info):
        print "\n\n\n EXEC SEED FINDER", terms, " ", data_path, " \n\n\n"
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
    
        csv_file = seed_dir + terms.replace(" ","_") + "_results.csv"
        
        if isfile(csv_file):
            remove(csv_file)

        ache_home = environ['ACHE_HOME']
        
        comm = ache_home + "/bin/ache seedFinder --csvPath " + csv_file + " --initialQuery \"" +terms + "\" --modelPath " + crawlermodel_dir + " --seedsPath " + seed_dir + " --maxPages 2 --maxQueries 25"

        encoded_query = urllib.quote(terms).replace("%5C","")

        f_sf_log = open(seed_dir+"log/seeds_"+"+".join(encoded_query.split("%20"))+".log", 'w')
        f_sf_err_log = open(seed_dir+"log/seeds_"+"+".join(encoded_query.split("%20"))+"_error.log", 'w')
        p = Popen(comm, shell=True, stderr=f_sf_err_log, stdout=f_sf_log)

        # Upload seeds generated by the SeedFinder
        CollectSeeds(terms, csv_file, es_info).collect_seed_urls(callback, p)

        


