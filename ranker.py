import bisect
import numpy as np
from numpy.linalg import norm
import utils



class Ranker:
    def __init__(self, avg_length, document_dict, config):
        self.avg_length_per_doc = avg_length
        self.document_dict = document_dict
        # self.loaded_doc_postings = {}  # key - doc_posting name, value - the posting itself
        self.loaded_doc_postings = {}  # key - tweet_id , value - the tweet's vector and the tweet_date
        self.config = config

    # @staticmethod
    def rank_relevant_doc(self, relevant_doc, query_glove_vec, query_tf_idf_vec):
        """
        This function provides rank for each relevant document and sorts them by their scores.
        The current score considers solely the number of terms shared by the tweet_id (full_text) and query.
        :param relevant_doc: dictionary of documents that contains at least one term from the query.
        :return: sorted list of documents by score
        """
        ret = []
        key_list = list(relevant_doc.keys())
        # for tweet_id, tuple_vec_doclength in relevant_doc.items():
        for idx, tweet_id in enumerate(key_list):
            tuple_vec_doclength = relevant_doc[tweet_id]
            # if self.document_dict[tweet_id] not in self.loaded_doc_postings:
            if tweet_id not in self.loaded_doc_postings:
                loaded_dict = utils.load_dict(self.document_dict[tweet_id], self.config.get_out_path())
                # self.loaded_doc_postings[self.document_dict[tweet_id]] = loaded_dict
                self.loaded_doc_postings[tweet_id] = loaded_dict[tweet_id]
                for i in range(idx+1, len(key_list)):
                    if key_list[i] in loaded_dict:
                        self.loaded_doc_postings[key_list[i]] = loaded_dict[key_list[i]]

            bm25_vec = tuple_vec_doclength[0]
            doc_length = tuple_vec_doclength[1]
            # glove_vec = tuple_vec_doclength[2]
            # glove_vec = self.loaded_doc_postings[self.document_dict[tweet_id]][tweet_id][0]
            glove_vec = self.loaded_doc_postings[tweet_id][0]
            # tweet_date = self.loaded_doc_postings[self.document_dict[tweet_id]][tweet_id][1]
            tweet_date = self.loaded_doc_postings[tweet_id][1]
            calculated_score = self.calc_score(bm25_vec, doc_length, glove_vec, query_glove_vec, query_tf_idf_vec)
            tweet_tuple = (calculated_score, tweet_id, tweet_date)
            bisect.insort(ret, tweet_tuple)

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
            return sorted(sorted_relevant_doc, key=lambda x: (x[0], x[2]))

        return sorted(sorted_relevant_doc, key=lambda x: (x[0], x[2]))[-k:]

    def calc_score(self, bm25_vec, doc_length, glove_vec, query_glove_vec, querty_tf_idf_vec):
        """

        :param query_vec:
        :param bm25_vec:
        :param glove_vec:
        :param vec: a 2xlen(query) numpy matrix, first row holds tf data,
                                           secoend row holds idf data
        :param doc_length:
        :return: calculated score of similarity between the represented tweet and the query
        """
        w_cos_weight = 0.9
        bm25_weight = 0.05
        glove_weight = 0.05

        word_cosine = w_cos_weight * self.cosine(bm25_vec[0] * bm25_vec[1], querty_tf_idf_vec[0] * querty_tf_idf_vec[1])
        bm25_score = bm25_weight * self.calc_BM25(bm25_vec, doc_length)
        glove_cosine = glove_weight * self.cosine(glove_vec, query_glove_vec)

        score = word_cosine + glove_cosine + bm25_score

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
        if denominator == 0 or numenator == 0:
            return 0
        return numenator / denominator
