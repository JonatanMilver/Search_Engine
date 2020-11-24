from parser_module import Parse
from ranker import Ranker
import numpy as np
import utils


class Searcher:

    def __init__(self, inverted_index):
        """
        :param inverted_index: dictionary of inverted index
        """
        self.parser = Parse()
        self.ranker = Ranker()
        self.inverted_index = inverted_index
        self.posting_file_dict = {}


    def relevant_docs_from_posting(self, query):
        """
        This function loads the posting list and count the amount of relevant documents per term.
        :param query: query
        :return: dictionary of relevant documents.
        """
        # posting = utils.load_obj("posting", '')
        relevant_docs = {}
        for idx, term in enumerate(query):
            try:  # an example of checks that you have to do
                if term in self.inverted_index:
                    curr_posting = utils.load_obj(self.inverted_index[term][1], '')
                    self.posting_file_dict[term] = curr_posting[term]
                    for i in range(idx+1, len(query)):
                        if query[i] in curr_posting:
                            self.posting_file_dict[query[i]] = curr_posting[query[i]]
                # posting_doc = posting[term]
            except:
                print('term {} not found in posting'.format(term))
        for idx, doc_list in enumerate(self.posting_file_dict.values()):
            try:
                for doc_tuple in doc_list:
                    tweet_id = doc_tuple[0]
                    if tweet_id not in relevant_docs.keys():
                        tf_idf_vec = np.zeros(shape=(len(query)))
                        # tf_idf_vec[idx] = # tf-idf
                        relevant_docs[tweet_id] = tf_idf_vec
                    else:
                        tf_idf_vec = relevant_docs[tweet_id]
                        # tf_idf_vec[idx] = #tf-idf
                        relevant_docs[tweet_id] = tf_idf_vec
            except:
                print('term {} not found in posting'.format(term))

        return relevant_docs
