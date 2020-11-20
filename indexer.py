import bisect
from collections import Counter


class Indexer:

    def __init__(self, config):
        # given a term, returns the number of doc/tweets in which he is in
        self.inverted_idx = {}
        self.document_dict = {}
        # postingDict[term] = [(d1.tweet_id, d1.number_of_appearances_in_doc, d1.locations of term in doc), (d2.tweet_id, d2.number_of_appearances_in_doc), ...]
        self.postingDict = {}
        self.entities_dict = Counter()
        self.config = config

    def add_new_doc(self, document):
        """
        This function perform indexing process for a document object.
        Saved information is captures via two dictionaries ('inverted index' and 'posting')
        :param document: a document need to be indexed.
        :return: -
        """

        document_dictionary = document.term_doc_dictionary
        document_capitals = document.capital_letter_indexer
        document_entities = document.named_entities
        for entity in document_entities:
            self.entities_dict[entity] += 1
        self.document_dict[document.tweet_id] = (document.tweet_date, document.doc_length,
                                                 document.max_tf, document.unique_terms)
        # Go over each term in the doc
        for term in document_dictionary.keys():
            # try:
            is_upper = False
            if term.upper() in self.inverted_idx:
                if term in document_capitals and document_capitals[term] is True:
                    is_upper = True
                    self.inverted_idx[term.upper()] += 1
                # elif term in document_capitals and document_capitals[term] is False:
                else:
                    # replace term.upper() with term in inverted_idx
                    # replace term.upper() with term in posting_files_dict
                    inverted_val = self.inverted_idx[term.upper()]
                    posting_val = self.postingDict[term.upper()]
                    del self.inverted_idx[term.upper()]
                    del self.postingDict[term.upper()]
                    self.inverted_idx[term] = inverted_val
                    self.postingDict[term] = posting_val
            elif term in self.inverted_idx:
                self.inverted_idx[term] += 1
            else:
                if term in document_capitals and document_capitals[term] is True:
                    is_upper = True
                if is_upper:
                    self.inverted_idx[term.upper()] = 1
                    self.postingDict[term.upper()] = []
                else:
                    self.inverted_idx[term] = 1
                    self.postingDict[term] = []

            insert_tuple = (document.tweet_id, len(document_dictionary[term]),
                            document_dictionary[term])
            if is_upper:
                term = term.upper()
            if len(self.postingDict[term]) == 0:
                self.postingDict[term].append(insert_tuple)
            else:
                self.insert_line_to_posting_dict(term, insert_tuple)

            # except Exception as e:
            #     # print('problem with the following key {}'.format(term[0]))
            #     print(e)

    def insert_line_to_posting_dict(self, term, t):
        # term_list = self.postingDict[term]
        # for idx, tup in enumerate(term_list):
        #     if int(t[0]) < int(tup[0]):
        #         term_list.insert(idx, t)
        #         return
        # term_list.append(t)
        bisect.insort(self.postingDict[term], t)
