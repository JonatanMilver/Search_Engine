import bisect
import numpy as np
from numpy.linalg import norm

class Ranker:
    def __init__(self, avg_length):
        self.avg_length_per_doc = avg_length

    # @staticmethod
    def rank_relevant_doc(self, relevant_doc, query_glove_vec, query_tf_idf_vec):
        """
        This function provides rank for each relevant document and sorts them by their scores.
        The current score considers solely the number of terms shared by the tweet (full_text) and query.
        :param relevant_doc: dictionary of documents that contains at least one term from the query.
        :return: sorted list of documents by score
        """
        ret = []
        for tweet, tuple_vec_doclength in relevant_doc.items():
            bm25_vec = tuple_vec_doclength[0]
            doc_length = tuple_vec_doclength[1]
            glove_vec = tuple_vec_doclength[2]
            tweet_tuple = (self.calc_score(bm25_vec, doc_length, glove_vec, query_glove_vec, query_tf_idf_vec, tweet), tweet)
            bisect.insort(ret, tweet_tuple)
            # bisect.insort()

        return ret

    @staticmethod
    def retrieve_top_k(sorted_relevant_doc, k=1):
        """
        return a list of top K tweets based on their ranking from highest to lowest
        :param sorted_relevant_doc: list of all candidates docs.
        :param k: Number of top document to return
        :return: list of relevant document
        """

        if k > len(sorted_relevant_doc):
            return sorted_relevant_doc

        return sorted_relevant_doc[-k:]

    def calc_score(self, bm25_vec, doc_length, glove_vec, query_glove_vec, querty_tf_idf_vec, tweet_id):
        """

        :param query_vec:
        :param bm25_vec:
        :param glove_vec:
        :param vec: a 2xlen(query) numpy matrix, first row holds tf data,
                                           secoend row holds idf data
        :param doc_length:
        :return: calculated score of similarity between the represented tweet and the query
        """
        bm25_score = self.calc_BM25(bm25_vec, doc_length)
        glove_cosine = self.cosine(glove_vec, query_glove_vec)
        word_cosine = self.cosine(bm25_vec[0]*bm25_vec[1], querty_tf_idf_vec[0]*querty_tf_idf_vec[1])
        score = word_cosine # * 0.8 + glove_cosine * 0.2 # * 0.7 + bm25_score * 0.15 + glove_cosine * 0.15
        # if score > 0.85:
        # print("{} : word cosine: {} , glove cosine: {}, total score {}".format(tweet_id,word_cosine,glove_cosine, score))
        return score
    def calc_BM25(self, vec, doc_length):
        # BM25 score calculation
        score = 0
        k = 1.2
        b = 0.75
        for column in vec.T:
            idf = column[1]
            tf = column[0]

            score += (idf * tf * (k + 1)) / (tf + k * (1 - b + b * (doc_length / self.avg_length_per_doc)))

        return score

    def cosine(self, v1, v2):
        numenator = np.dot(v1, v2)
        denominator = norm(v1) * norm(v2)
        return numenator / denominator
