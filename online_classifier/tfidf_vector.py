from sklearn.feature_extraction.text import TfidfTransformer
from nltk import corpus
import numpy as np

from tf_vector import tf_vectorizer

class tfidf_vectorizer(tf_vectorizer):
    
    def __init__(self, convert_to_ascii=False, max_features= 10000, ngram_range=(1,1)):
        self.tfidf_transformer = None
        tf_vectorizer.__init__(self, convert_to_ascii, max_features, ngram_range)
        
    def tfidf(self, data):
        [X_counts, features] = self.vectorize(data)
        if self.tfidf_transformer is None:
            self.tfidf_transformer = TfidfTransformer()
            X = self.tfidf_transformer.fit_transform(X_counts)
        else:
            X = self.tfidf_transformer.transform(X_counts)

        return [X, X_counts, features]

    @staticmethod
    def getTerms(corpus, indices):
        return [corpus[x] for x in indices]

    @staticmethod
    def getTopTerms(tfidfArray, corpus, num_docs, top):
        N = num_docs
        avg = np.sum(tfidfArray, axis=0)
        sortedAvgIndices = np.argsort(np.array(avg)[0])[::-1]
        return [corpus[i] for i in sortedAvgIndices[0:top]]
