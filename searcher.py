import math

from parser_module import Parse
from ranker import Ranker
import numpy as np
import utils


class Searcher:

    def __init__(self, inverted_index, n,  avg_length_per_doc, q):
        """
        :param inverted_index: dictionary of inverted index
        """
        self.parser = Parse()
        self.ranker = Ranker(avg_length_per_doc)
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

        qterm_to_idf = {}

        for idx, term in enumerate(query):
            try:  # an example of checks that you have to do
                if term in self.inverted_index:
                    qterm_to_idf[term] = self.calculate_idf(self.inverted_index[term])
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
                    qterm_to_idf[term] = 0
                    doc_list = None
                    idx_set = {idx}
                    self.term_to_doclist[term] = [idx_set, doc_list]

                # posting_doc = posting[term]
            except:
                print('term {} not found in inverted index'.format(term))

        relevant_docs = {}
        # for idx, doc in enumerate(self.term_to_doclist.items()):
        for term_to_docs in self.term_to_doclist.items():
            term = term_to_docs[0]
            term_indices = term_to_docs[1][0]
            doc_list = term_to_docs[1][1]

            try:
                if doc_list is not None:
                    for doc_tuple in doc_list:
                        tweet_id = doc_tuple[0]
                        # if tweet_id == "1287538255076835328":
                        #     print()
                        if tweet_id not in relevant_docs:
                            #[[tf1, tf2...]
                            # [idf1, idf2...]]
                            tf_idf_vec = np.zeros(shape=(2, len(query)))
                            relevant_docs[tweet_id] = (tf_idf_vec, doc_tuple[1])

                        vec = relevant_docs[tweet_id][0]
                        tf = self.calculate_tf(doc_tuple)
                        for index in term_indices:
                            vec[0, index] = tf
                        for idx, q_term in enumerate(query):
                            vec[1, idx] = qterm_to_idf[q_term]
                            # vec[1, index] = qterm_to_idf[term]
                        # relevant_docs[tweet_id] = (vec, doc_tuple[1])
            except:
                print('term {} not found in posting'.format(term))



        return relevant_docs

    # def relevant_docs_from_posting(self, query):
    #     """
    #     This function loads the posting list and count the amount of relevant documents per term.
    #     :param query: query
    #     :return: dictionary of relevant documents.
    #     """



    def calculate_tf(self, tweet_term_tuple):
        # to calc normalize tf
        # num_of_ocur_most_common_word_in_doc = tweet_term_tuple[2]
        num_of_terms_in_doc = tweet_term_tuple[1]
        frequency_term_in_doc = tweet_term_tuple[4]
        tf = frequency_term_in_doc / num_of_terms_in_doc

        return tf

    def calculate_idf(self, term_data):
        # to calc idf
        n = self.number_of_documents
        df = term_data[0]
        idf = math.log10(n / df)
        return idf
