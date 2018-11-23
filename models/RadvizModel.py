from radviz import Radviz
import  numpy as np
from collections import OrderedDict
import json

from online_classifier.tfidf_vector import tfidf_vectorizer
from domain_discovery_model import DomainModel
from elastic.config import es, es_doc_type, es_server
from elastic.get_config import get_available_domains, get_model_tags
from fetch_data import fetch_data

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.datasets import fetch_20newsgroups
from numpy.linalg import norm
from sklearn.metrics import accuracy_score
from sklearn.manifold import TSNE

from sklearn.cluster import KMeans
from sklearn.cluster import FeatureAgglomeration
from sklearn.metrics import pairwise_distances
from sklearn import svm
from sklearn.naive_bayes import MultinomialNB
from sklearn import linear_model

import random
from scipy.sparse import csr_matrix, lil_matrix
from functools import reduce
import nltk


#import urllib2
#from bs4 import BeautifulSoup

class RadvizModel(DomainModel):
    radviz = None

    def __init__(self, path):
        self._path = path
        super(RadvizModel, self).__init__(path)
        self._domains = get_available_domains(self._es)
        self._mapping = {"url":"url", "timestamp":"retrieved", "text":"text", "html":"html", "tag":"tag", "query":"query", "domain":"domain", "title":"title"}

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

    def clusterIndicesNumpy(self, clustNum, labels_array): #numpy
        return np.where(np.array(labels_array) == clustNum)[0]

    def Kmeans(self, data, nro_cluster ):
        random_state = 170
        vectorizer = TfidfVectorizer(max_features=2000)
        vectors = vectorizer.fit_transform(data)
        #convert sparce matrix to dense matrix
        sparceMatrix = vectors
        X_norm = (sparceMatrix - sparceMatrix.min())/(sparceMatrix.max() - sparceMatrix.min())
        denseMatrix = X_norm.todense()
        X = denseMatrix
        yPredKmeans = KMeans(n_clusters=nro_cluster, random_state=random_state).fit_predict(X) #Kmeans
        return yPredKmeans


    def getRandomSample_inCluster(self, nro_cluster, y_Pred, raw_data, labels):
        y_clusterData = range(nro_cluster)
        clusters = []
        labels_cluster =[]
        X_sum = []
        temp_data = []
        max_features = 200
        for i in range(nro_cluster):
            cluster = []
            idsData_cluster = self.clusterIndicesNumpy(i,y_Pred)
            for j in idsData_cluster:
                cluster.append( raw_data[j] )
            clusters.append(cluster)
            random_id = random.randint(0,len(idsData_cluster)-1)
            temp_data.append(raw_data[idsData_cluster[3]]) #3
            labels_cluster.append(labels[idsData_cluster[3]]) #3

            tf_v = tfidf_vectorizer(convert_to_ascii=True, max_features=max_features)
            [X, features] = tf_v.vectorize(clusters[i])
            temp = np.squeeze(np.asarray(np.sum(X.todense(), axis=0)))
            X_sum.append(np.ceil(temp))
        return [temp_data,labels_cluster, X_sum]

    def getClusterInfo(self, nro_cluster, y_Pred, raw_data, labels,  urls,titles, snippets,  image_urls, max_features):
        y_clusterData = range(nro_cluster)
        clusters_RawData = []
        label_by_clusters =[]
        original_labels=[]#Original labels but with different order (because now the order of data is defined by grouped clusters).
        original_urls = []#Original urls but with different order.
        original_titles = [] #Original titles but with different order.
        original_snippets = []#Original snippets but with different order.
        original_imageUrls = []#Original image_urls but with different order.
        subset_raw_data = []
        features_in_clusters =[]
        clusters_TFData = []
        for i in range(nro_cluster):
            cluster = []
            idsData_cluster = self.clusterIndicesNumpy(i,y_Pred)
            for j in idsData_cluster:
                cluster.append( raw_data[j] )
                original_labels.append(labels[j])
                original_urls.append(urls[j])
                original_titles.append(titles[j])
                original_snippets.append(snippets[j])
                original_imageUrls.append(image_urls[j])
            clusters_RawData.append(cluster)
            random_id = random.randint(0,len(idsData_cluster)-1)
            subset_raw_data.append(raw_data[idsData_cluster[3]]) #3
            #label_by_clusters.append(labels[idsData_cluster[3]]) #3
            label_by_clusters.append(i) #3

            # max_features set to twice required to filte using pos tags
            tf_v = tfidf_vectorizer(convert_to_ascii=True, max_features=2*max_features)
            [X, features] = tf_v.vectorize(clusters_RawData[i])

            # Apply parts of speech tagging to improve selected keywords
            posf_features, posf_index = self._pos_filter(features, max_features=max_features)
            X_dense = X.todense()[:,posf_index]

            clusters_TFData.append(X_dense)
            features_in_clusters.append(posf_features)


        # Co-cluster keywords using co-clustering formula
        w_d = {}
        X_sum = []
        updated_features_in_clusters = []
        updated_clusters_TFData = []
        for i in range(nro_cluster):
            curr_c = np.transpose(clusters_TFData[i])
            curr_w = features_in_clusters[i]
            new_features = []
            delete_feature_data = []
            for k in range(0, len(curr_w)):
                if w_d.get(curr_w[k]) is None:
                    w_d[curr_w[k]] = [-1]*nro_cluster
                if w_d[curr_w[k]][i] == -1:
                    w_d[curr_w[k]][i] = self._word_cluster_freq(curr_w[k], curr_w, curr_c)

                include_feature = True
                for j in range(0, nro_cluster):
                    if j != i:
                        if w_d[curr_w[k]][j] == -1:
                            w_d[curr_w[k]][j] = self._word_cluster_freq(curr_w[k], features_in_clusters[j], np.transpose(clusters_TFData[j]))
                        if w_d[curr_w[k]][i] < w_d[curr_w[k]][j]:
                            include_feature = False
                            break
                if include_feature:
                    new_features.append(curr_w[k])
                else:
                    delete_feature_data.append(k)

            updated_features_in_clusters.append(new_features)
            X = np.transpose(np.delete(curr_c, delete_feature_data, 0))
            print X.shape, ' ' , len(new_features)

            # zero_doc = np.where(~X.any(axis=1))
            # if zero_doc[0].size > 0:
            #     X = np.delete(X, zero_doc[0], 0)
            #     original_labels = np.delete(np.asarray(original_labels), zero_doc[0]).tolist()
            #     original_urls = np.delete(np.asarray(original_urls), zero_doc[0]).tolist()
            #     original_titles = np.delete(np.asarray(original_titles), zero_doc[0]).tolist()
            #     original_snippets = np.delete(np.asarray(original_snippets), zero_doc[0]).tolist()
            #     original_imageUrls = np.delete(np.asarray(original_imageUrls), zero_doc[0]).tolist()

            #     print np.transpose(curr_c)[zero_doc[0]]
            # print X.shape, ' ' , len(new_features)

            updated_clusters_TFData.append(X)
            temp = np.squeeze(np.asarray(np.sum(X, axis=0)))
            X_sum.append(np.ceil(temp))


        return [updated_features_in_clusters, clusters_RawData, label_by_clusters, original_labels,original_urls, original_titles, original_snippets,original_imageUrls, updated_clusters_TFData, X_sum, subset_raw_data ]


        #features_in_clusters : list of features for each cluster.
        #clusters_RawData : array of arrays. [[raw_data_for_cluster1][raw_data_for_cluster2][raw_data_for_cluster3] ...]
        #label_by_clusters : label for each cluster.
        #clusters_TFData : array of arrays. [[raw_data_for_cluster1][raw_data_for_cluster2][raw_data_for_cluster3] ...]
        #X_sum : the clusters_TFData (tf vectors) of each cluster was reduced to just one vector (the columns were summed). At the end only one vector is generated for each cluster.
        #subset_raw_data: sub dataset (raw data) from each cluster. A random sample was took from each cluster.

    def _pos_filter(self, docterms=[], pos_tags=['NN','NNS', 'NNP', 'NNPS', 'JJ'], max_features=200):
        tagged = nltk.pos_tag(docterms)
        valid_words = [tag[0] for tag in tagged if tag[1] in pos_tags][0:max_features]
        valid_words_index = [docterms.index(word) for word in valid_words]
        return valid_words, valid_words_index

    def _word_cluster_freq(self, word, features, w_d):

        if not word in features:
            return 0

        w_index = features.index(word)
        return np.sum(w_d[w_index:])

    def getClassInfo(self, label_by_classes, allLabels,  urls,titles, snippets,  image_urls, raw_data, max_features ):

        classes_TFData = []
        classes_RawData = []
        features_in_classes =[]
        original_labels = [] #Original labels but with different order.(because now the order of data is defined by grouped clusters).
        original_urls = []#Original urls but with different order.
        original_titles = [] #Original titles but with different order.
        original_snippets = []#Original snippets but with different order.
        original_imageUrls = []#Original image_urls but with different order.
        X_sum = []

        for i in label_by_classes:
            oneClass = []
            arr = [str(r) for r in allLabels]
            idsData_class = self.clusterIndicesNumpy(str(i),arr)
            for j in idsData_class:
                oneClass.append( raw_data[j] )
                original_labels.append(allLabels[j])
                original_urls.append(urls[j])
                original_titles.append(titles[j])
                original_snippets.append(snippets[j])
                original_imageUrls.append(image_urls[j])

            classes_RawData.append(oneClass)
            tf_v = tfidf_vectorizer(convert_to_ascii=True, max_features=max_features)
            [X, features] = tf_v.vectorize(oneClass)
            classes_TFData.append(X.todense())
            features_in_classes.append(features)

            temp = np.squeeze(np.asarray(np.sum(X.todense(), axis=0)))
            X_sum.append(np.ceil(temp))

        return [features_in_classes, classes_TFData, classes_RawData, original_labels,original_urls, original_titles, original_snippets,original_imageUrls,  label_by_classes ]
        #Important!! allLabels has a different order than original_labels. Since here, the order of original_labels array should be taken into account.
        #features_in_classes : list of features for each class.
        #label_by_classes : label for each class.
        #classes_TFData : array of arrays. [[raw_data_for_class1][raw_data_for_class2][raw_data_for_class3] ...]
        #X_sum : the classes_TFData (tf vectors) of each class was reduced to just one vector (the columns were summed). At the end only one vector is generated for each class.


    def getMedoidSamples_inCluster(self,nro_cluster, y_Pred, raw_data, labels,  urls,titles, snippets,  image_urls, max_features_in_cluster):

        max_features = max_features_in_cluster
        [features_in_clusters, clusters_RawData, label_by_clusters, original_labels, original_urls, original_titles, original_snippets,original_imageUrls, clusters_TFData, X_sum, subset_raw_data ] = self.getClusterInfo(nro_cluster, y_Pred, raw_data, labels,  urls,titles, snippets,  image_urls,  max_features)

        features_uniques = np.unique(features_in_clusters).tolist()
        cluster_labels = []
        new_X_sum = []
        for i in range(nro_cluster):
            new_X = np.zeros(len(features_uniques))
            for j in range(len(features_in_clusters[i])): #loop over the cluster's features
                try:
                    index_feat = features_uniques.index(features_in_clusters[i][j])
                    new_X[index_feat]=X_sum[i][j]
                except ValueError:
                    print "error"
            new_X_sum.append(np.asarray(new_X))
            cluster_labels.append(label_by_clusters[i])
        return [subset_raw_data,cluster_labels, new_X_sum, features_uniques]

    def getMedoidSamples_inCluster_NumData(self,features_in_clusters, features_uniques, label_by_clusters, X_sum ):

        features_uniques = np.unique(features_uniques).tolist()
        cluster_labels = []
        new_X_sum = []
        for i in range(nro_cluster):
            new_X = np.zeros(len(features_uniques))
            for j in range(len(features_in_clusters[i])): #loop over the cluster's features
                try:
                    index_feat = features_uniques.index(features_in_clusters[i][j])
                    new_X[index_feat]=X_sum[i][j]
                except ValueError:
                    print "error"
            new_X_sum.append(np.asarray(new_X))
            cluster_labels.append(label_by_clusters[i])
        return [cluster_labels, new_X_sum]

    def getVectors_for_allSamples(self, nro_cluster, clusters_TFData, features_uniques, features_in_clusters, label_by_clusters, subset_raw_data):
        new_X_sum = []
        cluster_labels = []
        for i in range(nro_cluster):
            X_from_cluster = clusters_TFData[i]
            for k in range(len(clusters_TFData[i])):
                new_X = np.zeros(len(features_uniques))
                tempList = np.squeeze(np.asarray(X_from_cluster[k]))
                for j in range(len(features_in_clusters[i])): #loop over the cluster's features
                    try:
                        index_feat = features_uniques.index(features_in_clusters[i][j])
                        new_X[index_feat]=tempList[j]
                    except ValueError:
                        print "error"
                new_X_sum.append(np.asarray(new_X))
                cluster_labels.append(label_by_clusters[i])
        return [subset_raw_data, cluster_labels, new_X_sum, features_uniques]

    def getVectors_for_allSamplesIntoClasses(self, features_uniques, features_in_classes, classes_TFData, classes_RawData, label_by_classes):
        new_X_sum = []
        new_X_sum_copy = []
        classes_labels = [] #Labels assigned for each data. This can be just one by data. By contrast, original_urls have the original labels could mean each data could have more than one label (ex. A data could be 'relevant' and 'deep crawling' labels ).

        for i in range(len(label_by_classes)):
            X_from_cluster = classes_TFData[i]
            for k in range(len(classes_TFData[i])):

                new_X = np.zeros(len(features_uniques))
                tempList = np.squeeze(np.asarray(X_from_cluster[k]))
                for j in range(len(features_in_classes[i])): #loop over the class's features
                    try:
                        index_feat = features_uniques.index(features_in_classes[i][j])
                        new_X[index_feat]=tempList[j]
                    except ValueError:
                        print "error"
                new_X_sum.append(np.asarray(new_X))
                new_X_sum_copy.append(np.asarray(new_X))
                classes_labels.append(label_by_classes[i])
        return [features_uniques, new_X_sum, new_X_sum_copy,  classes_labels ] #classes_labels should be equal to original_labels

    def getAllSamples_inCluster(self, nro_cluster, y_Pred, raw_data, labels,  urls,titles, snippets,  image_urls, max_features_in_cluster):
        max_features = max_features_in_cluster
        [features_in_clusters, clusters_RawData, label_by_clusters, original_labels, original_urls, original_titles, original_snippets,original_imageUrls, clusters_TFData, X_sum, subset_raw_data ] = self.getClusterInfo(nro_cluster, y_Pred, raw_data, labels,  urls,titles, snippets,  image_urls, max_features)

        #features_uniques = np.unique(features_in_clusters).tolist()
        concatenate_ = np.concatenate((features_in_clusters), axis=0) #concatenate all features from clusters into one single array
        features_uniques = np.unique(concatenate_).tolist() #remove duplicated features

        [subset_raw_data, cluster_labels, new_X_sum, features_uniques] = self.getVectors_for_allSamples(nro_cluster, clusters_TFData, features_uniques, features_in_clusters, label_by_clusters, subset_raw_data)
        return  [subset_raw_data, cluster_labels, new_X_sum, features_uniques, original_labels, original_urls, original_titles, original_snippets,original_imageUrls]

    def getAllSamples_inCluster_RemoveCommonFeatures(self, nro_cluster, y_Pred, raw_data, labels,  urls,titles, snippets,  image_urls,  max_features_in_cluster):
        max_features = max_features_in_cluster
        [features_in_clusters, clusters_RawData, label_by_clusters, original_labels, original_urls, original_titles, original_snippets,original_imageUrls, clusters_TFData, X_sum, subset_raw_data ] = self.getClusterInfo(nro_cluster, y_Pred, raw_data, labels, urls,titles, snippets,  image_urls,  max_features)

        concatenate_ = np.concatenate((features_in_clusters), axis=0) #concatenate all features from clusters into one single array
        features_uniques = np.unique(concatenate_).tolist() #remove duplicated features

        # Check if all docs are non-zero
        # for cluster in clusters_TFData:
        #     index = np.where(~cluster.any(axis=1))
        #     print index[0]
        #     if index[0].size > 0:
        #         print cluster[index[0][0]]

        [subset_raw_data, cluster_labels, new_X_sum, features_uniques] =  self.getVectors_for_allSamples(nro_cluster, clusters_TFData, features_uniques, features_in_clusters, label_by_clusters, subset_raw_data)

        return [subset_raw_data, cluster_labels, new_X_sum, features_uniques, original_labels, original_urls, original_titles, original_snippets,original_imageUrls]

    def getAllSamples_inClasses_RemoveCommonFeatures(self, label_by_classes, allLabels,  urls,titles, snippets,  image_urls, raw_data, max_features_in_cluster):
        max_features = max_features_in_cluster
        [features_in_classes, classes_TFData, classes_RawData, original_labels, original_urls, original_titles, original_snippets,original_imageUrls,  label_by_classes] = self.getClassInfo(label_by_classes, allLabels, urls,titles, snippets,  image_urls, raw_data, max_features )
        #print features_in_classes
        intersection = reduce(np.intersect1d, (features_in_classes)).tolist()#getting common keywords between all classes
        concatenate_ = np.concatenate((features_in_classes), axis=0) #concatenate all features from classes into one single array
        features_uniques_temp = np.unique(concatenate_).tolist() #remove duplicated features
        features_uniques = np.setdiff1d(features_uniques_temp,intersection).tolist() #removing common keywords between all classes
        [features_uniques, new_X_sum, new_X_sum_copy,classes_labels ] = self.getVectors_for_allSamplesIntoClasses(features_uniques, features_in_classes, classes_TFData, classes_RawData, label_by_classes)

        return [features_uniques, new_X_sum, new_X_sum_copy,classes_labels, original_labels, original_urls, original_titles, original_snippets,original_imageUrls]


    def getAllSamples_inCluster_RemoveCommonFeatures_medoids(self, nro_cluster, y_Pred, raw_data, labels, max_features_in_cluster):
        max_features = max_features_in_cluster
        [features_in_clusters, clusters_RawData, label_by_clusters, original_labels, original_urls, original_titles, original_snippets,original_imageUrls,clusters_TFData, X_sum, subset_raw_data ] = self.getClusterInfo(nro_cluster, y_Pred, raw_data, labels,  urls,titles, snippets,  image_urls,  max_features)

        intersection = reduce(np.intersect1d, (features_in_clusters)).tolist() #getting common keywords between all clusters
        #features_uniques_temp = np.unique(features_in_clusters).tolist()
        concatenate_ = np.concatenate((features_in_clusters), axis=0) #concatenate all features from clusters into one single array
        features_uniques_temp = np.unique(concatenate_).tolist() #remove duplicated features
        features_uniques = np.setdiff1d(features_uniques_temp,intersection).tolist()#removing common keywords between all clusters

        [labels_medoid_cluster, X_medoid_Cluster] = self.getMedoidSamples_inCluster_NumData(features_in_clusters, features_uniques, label_by_clusters, X_sum )
        [subset_raw_data, cluster_labels, new_X_sum, features_uniques] =  self.getVectors_for_allSamples(nro_cluster, clusters_TFData, features_uniques, features_in_clusters, label_by_clusters, subset_raw_data)
        return [subset_raw_data, cluster_labels, original_labels, new_X_sum, features_uniques,    labels_medoid_cluster, X_medoid_Cluster]

    def getNumberClasses(self, allLabels):
        uniqueLabels = np.array(np.unique(allLabels).tolist())
        return uniqueLabels

    def getData(self, typeRadViz):
        categories =  ['sci.crypt', 'rec.sport.hockey', 'talk.politics.mideast', 'soc.religion.christian']#'comp.os.ms-windows.misc', 'sci.med'
        newsgroups_train = fetch_20newsgroups(subset='train', categories=categories)
        data_train = newsgroups_train.data
        newsgroups_test = fetch_20newsgroups(subset='test', categories=categories)
        data_test = newsgroups_test.data
        labels = []
        labels.append(np.array(map(str, np.array(newsgroups_train.target))) )
        labels.append(np.array(map(str, np.array(newsgroups_test.target))) )
        temp_labels = []
        temp_urls = []
        for i in range(2):
            temp = []
            if i==1:
                temp = [w.replace('0', 'Neutral') for w in labels[i]] #comp.os.ms-windows.misc
                temp = [w.replace('1', 'Neutral') for w in temp]
                temp = [w.replace('2', 'Neutral') for w in temp]
                temp = [w.replace('3', 'Neutral') for w in temp]
            else:
                temp = [w.replace('0', 'rec.sport.hockey') for w in labels[i]] #comp.os.ms-windows.misc
                temp = [w.replace('1', 'sci.crypt') for w in temp]
                temp = [w.replace('2', 'soc.religion.christian') for w in temp]
                temp = [w.replace('3', 'talk.politics.mideast') for w in temp]
            temp_labels.append(temp)
            temp_urls.append(np.asarray(range(len(temp))).astype(str).tolist() ) #generating ids
        return [ data_train, data_test, temp_labels[0], temp_labels[1], temp_urls[0], temp_urls[1], temp_labels[0], temp_labels[1], temp_labels[0], temp_labels[1], temp_labels[0], temp_labels[1], temp_labels[0], temp_labels[1] ] # urls, titles, snippets, image_urls ]

    def getUniquesLabels(self, urls, labels):
        unique_labels = []
        one_label_byData = []
        for i in range(len(urls)):
            temp_labels = labels[i].split(',')
            if len(temp_labels)>1:
                value if (temp_labels[0].lower() != 'neutral') else temp_labels[1]
            else:
                value = temp_labels[0]
            if not value in unique_labels:
                unique_labels.append(value)
            one_label_byData.append(value)
        return [unique_labels, one_label_byData]

    def getClassData(self, raw_data, labels, urls, titles, snippets, image_urls):
        [unique_labels, one_label_byData] = self.getUniquesLabels(urls, labels)
        #print "unique_labels"
        #print one_label_byData
        #print len(one_label_byData)
        data_train = []
        data_test = []
        labels_train = []
        labels_test = []
        urls_train = []
        urls_test = []
        titles_train = []
        titles_test = []
        snippets_train = []
        snippets_test = []
        image_urls_train = []
        image_urls_test = []
        unique_labels_train = []
        unique_labels_test  = []

        for i in range(len(urls)):
            if one_label_byData[i].lower() != "neutral":
                data_train.append(raw_data[i])
                labels_train.append(labels[i])
                urls_train.append(urls[i])
                titles_train.append(titles[i])
                snippets_train.append(snippets[i])
                image_urls_train.append(image_urls[i])
                unique_labels_train.append(one_label_byData[i])
            else:
                data_test.append(raw_data[i])
                labels_test.append('Neutral')
                urls_test.append(urls[i])
                titles_test.append(titles[i])
                snippets_test.append(snippets[i])
                image_urls_test.append(image_urls[i])
                unique_labels_test.append('Neutral')
        return [ data_train, data_test, labels_train, labels_test, urls_train, urls_test, titles_train, titles_test, snippets_train, snippets_test, image_urls_train, image_urls_test, unique_labels_train, unique_labels_test ]

    def applyModel(self, new_X, features, newLabels,  data_test, labels_test, urls_test, titles_test, snippets_test, image_urls_test):
            pp = csr_matrix(new_X)
            sparceMatrix = pp
            X_norm = (sparceMatrix - np.array(sparceMatrix.min()))/(np.array(sparceMatrix.max()) - np.array(sparceMatrix.min()))
            denseMatrix = X_norm.todense() #convert sparce matrix to dense matrix
            X_aux = denseMatrix

            clf = MultinomialNB().fit(X_aux, newLabels)
            #clf_SGDC = linear_model.SGDClassifier().fit(X_aux, newLabels)
            #clf_SVM = svm.LinearSVC().fit(X_aux, newLabels)

            tf_v = tfidf_vectorizer(convert_to_ascii=True, max_features=len(features), vocabulary=features)
            [X_newdata, features_newfeatures] = tf_v.vectorize(data_test)

            predicted = clf.predict(X_newdata)
            #predicted_SGDC = clf_SGDC.predict(X_newdata)
            #predicted_SVM = clf_SVM.predict(X_newdata.todense())
            #predicted_test = clf_test.predict(X_newdata_test)

            X_test = X_newdata

            labels = labels_test
            urls = urls_test
            titles = titles_test
            snippets = snippets_test
            image_urls = image_urls_test
            cluster_labels = predicted.tolist()

            #accuracy_test = accuracy_score(labels_test, predicted)
            #print "Accuracy (classification): ", accuracy_test
            #accuracy_test_SGDC = accuracy_score(stringArray_test, predicted_SGDC)
            #accuracy_test_SVM = accuracy_score(stringArray_test, predicted_SVM)
            #accuracy_test_test = accuracy_score(stringArray_test, predicted_test)

            return [X_test, labels, urls, titles, snippets, image_urls, cluster_labels]

    def getRadvizPoints(self, session, filterByTerm, typeRadViz, nroCluster, removeKeywords):
        es_info = self._esInfo(session['domainId'])
        index = es_info['activeDomainIndex']
        max_features = 200

        ddteval_data = fetch_data(index, filterByTerm, removeKeywords, es_doc_type=es_doc_type, es=es)
        if session['domainId'] == "AV9z2HmeoAktwC6sp6_q":
            [ data_train, data_test, labels_train, labels_test, urls_train, urls_test, titles_train, titles_test, snippets_train, snippets_test, image_urls_train, image_urls_test, unique_labels_train, unique_labels_test ] = self.getData(typeRadViz)
            data = data_train
            labels = unique_labels_train
            urls = urls_train
            titles = titles_train
            snippets = snippets_train
            image_urls = image_urls_train
        else:
            data = ddteval_data["data"]
            labels = ddteval_data["labels"]
            urls = ddteval_data["urls"]
            titles = ddteval_data["title"]
            snippets = ddteval_data["snippet"]
            image_urls = ddteval_data["image_url"]

            [ data_train, data_test, labels_train, labels_test, urls_train, urls_test, titles_train, titles_test, snippets_train, snippets_test, image_urls_train, image_urls_test, unique_labels_train, unique_labels_test ] = self.getClassData(data, labels, urls, titles, snippets, image_urls)
            #data = data_train
            #labels = unique_labels_train
            #urls = urls_train
            #titles = titles_train
            #snippets = snippets_train
            #image_urls = image_urls_train

        #print labels

        X = []
        X_test = []
        features = []

        nro_cluster = int(nroCluster)
        max_anchors = 240
        max_features_in_cluster=int(np.ceil(max_anchors/nro_cluster))
        yPredKmeans = self.Kmeans(data, nro_cluster )
        #print yPredKmeans
        if typeRadViz == "1":
            tf_v = tfidf_vectorizer(convert_to_ascii=True, max_features=max_features)
            [X_, features_] = tf_v.vectorize(data)
            X = X_
            X_test = X_
            features = features_
            cluster_labels = labels
            newLabels = labels
            newUrls = urls
            newTitles = titles
            newSnippets = snippets
            newImageUrls = image_urls
        elif typeRadViz == "2":
            #[clusterData, labels_cluster, X_sum] = self.getRandomSample_inCluster( nro_cluster, yPredKmeans, data, labels)
            [clusterData,  cluster_labels, X_sum, features_uniques, newLabels, newUrls, newTitles, newSnippets,newImageUrls] = self.getAllSamples_inCluster( nro_cluster, yPredKmeans, data, labels,  urls,titles, snippets,  image_urls, max_features_in_cluster)
            data = clusterData
            features = features_uniques
            X = csr_matrix(X_sum)
            X_test = X
        elif typeRadViz == "3":
            [clusterData,  cluster_labels, X_sum, features_uniques, newLabels, newUrls, newTitles, newSnippets,newImageUrls] = self.getAllSamples_inCluster_RemoveCommonFeatures( nro_cluster, yPredKmeans, data, labels, urls,titles, snippets,  image_urls,  max_features_in_cluster)
            data = clusterData
            features = features_uniques
            X = csr_matrix(X_sum)
            X_test = X
            
        elif typeRadViz == "4":
            [clusterData,  cluster_labels, X_sum, features_uniques, newLabels, newUrls, newTitles, newSnippets,newImageUrls] = self.getAllSamples_inCluster_RemoveCommonFeatures( nro_cluster, yPredKmeans, data, labels, urls,titles, snippets,  image_urls,  max_features_in_cluster)
            data = clusterData
            features = features_uniques
            X = csr_matrix(X_sum)
            X_test = X
        # elif typeRadViz == "4":
        #     [clusterData,  cluster_labels, X_sum, features_uniques, newLabels, newUrls, newTitles, newSnippets,newImageUrls] = self.getAllSamples_inCluster_RemoveCommonFeatures( nro_cluster, yPredKmeans, data, labels, urls,titles, snippets,  image_urls,  max_features_in_cluster)
        #     data = clusterData
        #     features = features_uniques
        #     X = csr_matrix(X_sum)
        #     X_test = X
        elif typeRadViz == "5":
            label_by_classes = self.getNumberClasses(labels)
            [features_uniques, new_X_sum,new_X_sum_copy, classes_labels, newLabels, newUrls, newTitles, newSnippets,newImageUrls]  = self.getAllSamples_inClasses_RemoveCommonFeatures(label_by_classes, labels, urls,titles, snippets,  image_urls,  data, max_features_in_cluster)
            cluster_labels1 = classes_labels
            features = features_uniques
            X = csr_matrix(new_X_sum)

            [X_test_, labels_, urls_, titles_, snippets_, image_urls_, cluster_labels_] = self.applyModel(new_X_sum_copy, features, newLabels,  data, labels, urls,  titles, snippets, image_urls)
            newLabels = labels_
            newUrls = urls_
            newTitles = titles_
            newSnippets = snippets_
            newImageUrls = image_urls_
            cluster_labels  = cluster_labels_

            X_test = X_test_


        else:
            print "Nothing to do"

        matrix_transpose = np.transpose(X_test.todense())
        print "\n\n Number of 1-gram features = ", len(features)
        print "\n\n tf 1-gram matrix size = ", np.shape(X_test)
        # data = self.radviz.loadData_pkl("data/ht_data_200.pkl").todense()

        # data = np.transpose(data)

        # features = self.radviz.loadFeatures("data/ht_data_features_200.csv")
        # print features
        # print len(features)
        # labels = self.radviz.loadLabels("data/ht_data_labels_200.csv")
        # urls = self.radviz.loadSampleNames("data/ht_data_urls_200.csv")
        labels = newLabels
        urls = newUrls
        titles = newTitles
        snippets = newSnippets
        image_urls = newImageUrls

        #urls = np.asarray(range(len(stringArray))).astype(str).tolist() #generating ids
        #labels = stringArray
        #titles = stringArray
        #snippets = stringArray
        #image_urls = stringArray

        self.radviz = Radviz(X, features, labels, urls)

        return_obj = {}
        for i in range(0, len(features)):
            return_obj[features[i]] = matrix_transpose[i,:].tolist()[0]
        #labels_urls = OrderedDict([("labels",labels), ("urls",urls), ("title", ddteval_data["title"]),("snippet",ddteval_data["snippet"]),("image_url",ddteval_data["image_url"])])
        labels_urls = OrderedDict([("labels",labels), ("urls",urls), ("title", titles),("snippet",snippets),("image_url",image_urls), ("pred_labels",cluster_labels)])
        od = OrderedDict(list(OrderedDict(sorted(return_obj.items())).items()) + list(labels_urls.items()))
        return od

    def computeTSP(self):
        return self.radviz.compute_tsp()
