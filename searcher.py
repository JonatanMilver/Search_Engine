import math
from parser_module import Parse
from ranker import Ranker
import numpy as np
import utils


class Searcher:

    def __init__(self, inverted_index, document_dict, n,  avg_length_per_doc, glove_dict):
        """
        :param inverted_index: dictionary of inverted index
        """
        self.parser = Parse()
        self.ranker = Ranker(avg_length_per_doc)
        self.inverted_index = inverted_index
        self.document_dict = document_dict
        self.term_to_doclist = {}
        self.number_of_documents = n
        self.glove_dict = glove_dict

    def relevant_docs_from_posting(self, query):
        """
        This function loads the posting list and count the amount of relevant documents per term.
        :param query: query
        :return: dictionary of relevant documents.
        """
        qterm_to_idf = {}
        query_glove_vec = np.zeros(shape=25)
        query_vec = np.zeros(shape=(2, len(query)))
        for idx, term in enumerate(query):
            if term in self.glove_dict:
                query_glove_vec += self.glove_dict[term]
            try:  # an example of checks that you have to do
                if term in self.inverted_index:

                    qterm_to_idf[term] = self.calculate_idf(self.inverted_index[term])
                    query_vec[1, idx] = qterm_to_idf[term]
                    if term not in self.term_to_doclist:
                        # all documents having this term is not in the term dict,
                        # so load the appropriate postings and load them
                        curr_posting = utils.load_dict(self.inverted_index[term][1], '')

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

                else:  # term is un-known, so
                    qterm_to_idf[term] = 0
                    doc_list = None
                    idx_set = {idx}
                    self.term_to_doclist[term] = [idx_set, doc_list]

            except:
                print('term {} not found in inverted index'.format(term))
        query_glove_vec /= len(query)
        relevant_docs = {}
        for term_to_docs in self.term_to_doclist.items():
            term = term_to_docs[0]

            term_indices = term_to_docs[1][0]
            doc_list = term_to_docs[1][1]

            try:
                if doc_list is not None:
                    for doc_tuple in doc_list:
                        tweet_id = doc_tuple[0]
                        if tweet_id not in relevant_docs:
                            # example - > tf_idf_vec
                            # [[tf1, tf2...]
                            #  [idf1, idf2...]]
                            tf_idf_vec = np.zeros(shape=(2, len(query)))
                            relevant_docs[tweet_id] = (tf_idf_vec, self.document_dict[tweet_id][1],
                                                       self.document_dict[tweet_id][0])

                        vec = relevant_docs[tweet_id][0]
                        tf = self.calculate_tf(doc_tuple)
                        for index in term_indices:
                            vec[0, index] = tf
                        for idx, q_term in enumerate(query):
                            vec[1, idx] = qterm_to_idf[q_term]
                            query_vec[0, idx] = len(self.term_to_doclist[q_term][0]) / len(query)
            except:
                print('term {} not found in posting'.format(term))

        return relevant_docs, query_glove_vec, query_vec

    def calculate_tf(self, tweet_term_tuple):
        """
        calculates term frequency.
        :param tweet_term_tuple: tuple containing all information of the tweet of the term.
        :return:
        """
        # to calc normalize tf
        num_of_terms_in_doc = self.document_dict[tweet_term_tuple[0]][1]
        frequency_term_in_doc = tweet_term_tuple[3]
        tf = frequency_term_in_doc / num_of_terms_in_doc

        return tf

    def calculate_idf(self, term_data):
        """
        calculates idf of term
        :param term_data: term information
        :return:
        """
        # to calc idf
        n = self.number_of_documents
        df = term_data[0]
        idf = math.log10(n / df)
        return idf

    def calculate_idf_BM25(self, term_data):
        """
        calculates idf according to BM25 algorithm.
        :param term_data:
        :return:
        """
        n = self.number_of_documents
        df = term_data[0]
        idf = math.log(((n - df + 0.5) / (df + 0.5)) + 1)
        return idf
