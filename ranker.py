import bisect
import numpy as np

class Ranker:
    def __init__(self, avg_length):
        self.avg_length_per_doc = avg_length

    # @staticmethod
    def rank_relevant_doc(self, relevant_doc):
        """
        This function provides rank for each relevant document and sorts them by their scores.
        The current score considers solely the number of terms shared by the tweet (full_text) and query.
        :param relevant_doc: dictionary of documents that contains at least one term from the query.
        :return: sorted list of documents by score
        """
        ret = []
        for tweet, tuple_vec_doclength in relevant_doc.items():
            # tweet_tuple = (tweet, self.calc_score(vec))
            if tweet == "1287584690585710599":
                print()
            vec = tuple_vec_doclength[0]
            doc_length = tuple_vec_doclength[1]
            tweet_tuple = (self.calc_score(vec, doc_length), tweet)
            bisect.insort(ret, tweet_tuple)
            # bisect.insort()

        # return sorted(relevant_doc.items(), key=lambda item: item[1], reverse=True)
        return ret

    @staticmethod
    def retrieve_top_k(sorted_relevant_doc, k=1):
        """
        return a list of top K tweets based on their ranking from highest to lowest
        :param sorted_relevant_doc: list of all candidates docs.
        :param k: Number of top document to return
        :return: list of relevant document
        """
        return sorted_relevant_doc[-k:]

    def calc_score(self, vec, doc_length):
        """

        :param vec: a 2xlen(query) numpy matrix, first row holds tf data,
                                           secoend row holds idf data
        :param doc_length:
        :return: calculated score of similarity between the represented tweet and the query
        """
        score = 0
        k = 1.2
        b = 0.75
        for column in vec.T:
             idf = column[1]
             tf = column[0]

             score += (idf * tf * (k+1)) / (tf + k * (1-b + b*(doc_length/self.avg_length_per_doc)))

        return score

    def cosine(v1, v2):
        try:
            numenator = np.dot(v1, v2)
        except:
            print()
        denominator = np.norm(v1) * np.norm(v2)
        return numenator / denominator