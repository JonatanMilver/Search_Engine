import bisect

class Indexer:

    def __init__(self, config):
        # given a term, returns the number of doc/tweets in which he is in
        self.inverted_idx = {}
        self.document_dict = {}
        # postingDict[term] = [(d1.tweet_id, d1.number_of_appearances_in_doc, d1.locations of term in doc), (d2.tweet_id, d2.number_of_appearances_in_doc), ...]
        self.postingDict = {}
        self.config = config

    def add_new_doc(self, document):
        """
        This function perform indexing process for a document object.
        Saved information is captures via two dictionaries ('inverted index' and 'posting')
        :param document: a document need to be indexed.
        :return: -
        """

        document_dictionary = document.term_doc_dictionary
        self.document_dict[document.tweet_id] = (document.tweet_date, document.doc_length,
                                                 document.max_tf, document.unique_terms)
        # Go over each term in the doc
        for term in document_dictionary.keys():
            try:
                # Update inverted index and posting
                if term not in self.inverted_idx.keys():
                    self.inverted_idx[term] = 1
                    self.postingDict[term] = []
                else:
                    self.inverted_idx[term] += 1

                insert_tuple = (document.tweet_id, len(document_dictionary[term]),
                                               document_dictionary[term])
                if len(self.postingDict[term]) == 0:
                    self.postingDict[term].append(insert_tuple)
                else:
                    self.insert_line_to_posting_dict(term, insert_tuple)

            except:
                print('problem with the following key {}'.format(term[0]))

    def insert_line_to_posting_dict(self, term, t):
        # term_list = self.postingDict[term]
        # for idx, tup in enumerate(term_list):
        #     if int(t[0]) < int(tup[0]):
        #         term_list.insert(idx, t)
        #         return
        # term_list.append(t)
        bisect.insort(self.postingDict[term], t)
