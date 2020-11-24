import math

from parser_module import Parse
from ranker import Ranker
import numpy as np
import utils


class Searcher:

    def __init__(self, inverted_index, n):
        """
        :param inverted_index: dictionary of inverted index
        """
        self.parser = Parse()
        self.ranker = Ranker()
        self.inverted_index = inverted_index
        self.term_to_doclist = {}
        self.number_of_documents = n

    def relevant_docs_from_posting(self, query):
        """
        This function loads the posting list and count the amount of relevant documents per term.
        :param query: query
        :return: dictionary of relevant documents.
        """
        # posting = utils.load_obj("posting", '')

        for idx, term in enumerate(query):
            try:  # an example of checks that you have to do
                if term in self.inverted_index:
                    if term not in self.term_to_doclist:
                        # all documents having this term is not in the term dict,
                        # so load the appropriate postings and load them
                        curr_posting = utils.load_obj(self.inverted_index[term][1], '')
                        doc_list = curr_posting[term]
                        idx_set = {idx}
                        self.term_to_doclist[term] = [idx_set, doc_list]
                        for i in range(idx+1, len(query)):  # check if any other terms in query are the same posting to avoid loading it more than once
                            if query[i] in curr_posting:
                                doc_list = curr_posting[query[i]]
                                idx_set = {i}
                                self.term_to_doclist[query[i]] = [idx_set, doc_list]

                    else:  # term already in term dict, so only update it's index list
                        self.term_to_doclist[term][0].add(idx)

                else:  # term is un-knwon, so
                    doc_list = None
                    idx_set = {idx}
                    self.term_to_doclist[term] = [idx_set, doc_list]

                # posting_doc = posting[term]
            except:
                print('term {} not found in posting'.format(term))

        relevant_docs = {}
        # for idx, doc in enumerate(self.term_to_doclist.items()):
        for doc in self.term_to_doclist.items():
            term = doc[0]
            term_indices = doc[1][0]
            doc_list = doc[1][1]
            try:
                if doc_list is not None:
                    for doc_tuple in doc_list:
                        tweet_id = doc_tuple[0]
                        if tweet_id not in relevant_docs:
                            tf_idf_vec = np.zeros(shape=(len(query)))
                            relevant_docs[tweet_id] = tf_idf_vec

                        vec = relevant_docs[tweet_id]
                        tf_idf = self.calculate_tf_idf(doc_tuple, self.inverted_index[term])
                        for index in term_indices:
                            vec[index] = tf_idf
                        relevant_docs[tweet_id] = vec
            except:
                print('term {} not found in posting'.format(term))



        return relevant_docs

    def calculate_tf_idf(self, tweet_term_tuple, term_data):
        # to calc normalize tf
        # num_of_ocur_most_common_word_in_doc = tweet_term_tuple[2]
        num_of_terms_in_doc = tweet_term_tuple[1]
        frequency_term_in_doc = tweet_term_tuple[4]
        tf = frequency_term_in_doc / num_of_terms_in_doc

        # to calc idf
        n = self.number_of_documents
        df = term_data[0]
        idf = math.log10(n/df)

        res = tf * idf

        return res
