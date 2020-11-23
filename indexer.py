import bisect
from collections import Counter, OrderedDict
import utils


class Indexer:

    # global variables
    # POSTINGSDICTSIZE = 10000

    def __init__(self, config):
        # given a term, returns the number of doc/tweets in which he is in
        self.inverted_idx = {}

        # postingDict[term] = [(d1.tweet_id, d1.number_of_appearances_in_doc, d1.locations of term in doc), (d2.tweet_id, d2.number_of_appearances_in_doc), ...]
        self.postingDict = {}
        self.postingDict_size = 50000
        self.counter_of_postings = 1
        self.global_capitals = {}

        self.entities_dict = Counter()
        self.config = config

        self.merged_dicts = []

    def add_new_doc(self, document):
        """
        This function perform indexing process for a document object.
        Saved information is captures via two dictionaries ('inverted index' and 'posting')
        :param document: a document need to be indexed.
        :return: -
        """

        document_dictionary = document.term_doc_dictionary
        document_capitals = document.capital_letter_indexer
        for key_term in document_capitals:
            if key_term not in self.global_capitals:
                self.global_capitals[key_term] = document_capitals[key_term]
            else:
                if not document_capitals[key_term]:
                    self.global_capitals[key_term] = False

        document_entities = document.named_entities

        for entity in document_entities:
            self.entities_dict[entity] += 1

        # self.document_dict[document.tweet_id] = (document.tweet_date, document.doc_length,
        #                                          document.max_tf, document.unique_terms)

        # Go over each term in the doc
        for term in document_dictionary.keys():
            try:
                is_upper = False
                # if term.upper() in self.inverted_idx:
                #     if term.upper() not in self.postingDict:
                #         self.postingDict[term.upper()] = []
                #
                #     if term in document_capitals and document_capitals[term] is True:
                #         is_upper = True
                #         self.inverted_idx[term.upper()][0] += 1
                #
                #     else:
                #         # replace term.upper() with term in inverted_idx
                #         # replace term.upper() with term in posting_files_dict
                #         inverted_val = self.inverted_idx[term.upper()]
                #         posting_val = self.postingDict[term.upper()]
                #         del self.inverted_idx[term.upper()]
                #         del self.postingDict[term.upper()]
                #         # self.inverted_idx[term] = inverted_val
                #         # self.postingDict[term] = posting_val
                #         self.inverted_idx[term.lower()] = inverted_val
                #         self.postingDict[term.lower()] = posting_val

                # elif term in self.inverted_idx:
                if term in self.inverted_idx:
                    if term not in self.postingDict:
                        # self.postingDict[term] = []
                        # self.postingDict[term.lower()] = []
                        self.postingDict[term] = []
                    # self.inverted_idx[term][0] += 1
                    # self.inverted_idx[term.lower()][0] += 1
                    self.inverted_idx[term][0] += 1

                else:
                    # if term in document_capitals and document_capitals[term] is True:
                    #     is_upper = True
                    # if is_upper:
                    #     self.inverted_idx[term.upper()] = [1, str(self.counter_of_postings)]
                    #     self.postingDict[term.upper()] = []
                    # else:
                        # self.inverted_idx[term] = [1, str(self.counter_of_postings)]
                        # self.postingDict[term] = []
                        # self.inverted_idx[term.lower()] = [1, str(self.counter_of_postings)]
                    self.inverted_idx[term] = [1, str(self.counter_of_postings)]
                        # self.postingDict[term.lower()] = []
                    self.postingDict[term] = []


                insert_tuple = (document.tweet_id,  # tweet id
                                document.doc_length,  # total number of words in tweet
                                document.max_tf,  # number of occurrences of most common term in tweet
                                document.unique_terms,  # number of unique words in tweet
                                len(document_dictionary[term]),  # number of times term is in tweet
                                document_dictionary[term]  # positions of term in tweet
                                )

                # if is_upper:
                #     term = term.upper()
                # else:
                #     term = term.lower()
                if len(self.postingDict[term]) == 0:
                    self.postingDict[term].append(insert_tuple)
                else:
                    self.insert_line_to_posting_dict(term, insert_tuple)

                # check if posting_dict is full (>10,000 terms)
                if len(self.postingDict) == self.postingDict_size:
                    # for key, val in self.postingDict.items():
                    #     print(key, val)
                    self.save_postings()

            except:
                print('problem with the following key {}'.format(term))

    # def insert_line_to_posting_dict(self, term, t):
    #     posting_file_name = self.inverted_idx[term][1]
    #     if self.counter_of_postings == int(posting_file_name):
    #         bisect.insort(self.postingDict[term], t)
    #     else:
    #         print("in")
    #         old_post_dict = utils.load_obj(posting_file_name, '')
    #         bisect.insort(old_post_dict[term], t)
    #         utils.save_obj(old_post_dict, posting_file_name, '')

    def insert_line_to_posting_dict(self, term, t):
        bisect.insort(self.postingDict[term], t)

    # def remove_one_time_entities(self):
    #     for key in self.entities_dict:
    #         if self.entities_dict[key] < 2:

    def save_postings(self):
        o = OrderedDict(sorted(self.postingDict.items(), key=lambda x: x[0]))
        self.merged_dicts.append([str(self.counter_of_postings)])
        utils.save_obj(o, str(self.counter_of_postings), self.config.get_out_path())
        self.counter_of_postings += 1
        self.postingDict = {}

    def first(self):
        while len(self.merged_dicts) > 1:
            self.merged_dicts = self.second()

    # def second(self):
    #         new_merged = []
    #         i = 0
    #         while i < len(self.merged_dicts):
    #             if i + 1 >= len(self.merged_dicts):
    #                 new_merged.append(self.merged_dicts[i])
    #                 return new_merged
    #
    #             # posting files names will be added to this list
    #             added_dicts = []
    #
    #             list_1 = self.merged_dicts[i]
    #             list_2 = self.merged_dicts[i + 1]
    #
    #             index_l_1 = 0
    #             index_l_2 = 0
    #
    #             is_done_1 = False
    #             is_done_2 = False
    #
    #             d1 = utils.load_obj(list_1[index_l_1], '')
    #             d2 = utils.load_obj(list_2[index_l_2], '')
    #             index_d1 = 0
    #             index_d2 = 0
    #
    #             d1_keys = list(d1.keys())
    #             d2_keys = list(d2.keys())
    #
    #             new_dict = OrderedDict()
    #
    #             while index_l_1 < len(list_1) and index_l_2 < len(list_2):
    #                 if is_done_1:
    #                     d1 = utils.load_obj(list_1[index_l_1], '')
    #                     is_done_1 = False
    #                     index_d1 = 0
    #                 if is_done_2:
    #                     d2 = utils.load_obj(list_2[index_l_2], '')
    #                     is_done_2 = False
    #                     index_d2 = 0
    #
    #                 d1_keys = list(d1.keys())
    #                 d2_keys = list(d2.keys())
    #
    #                 d1_key = d1_keys[index_d1]
    #                 d1_in = d1_key
    #                 d2_key = d2_keys[index_d2]
    #                 d2_in = d2_key
    #                 while index_d1 < len(d1) and index_d2 < len(d2):
    #                     d1_key = d1_keys[index_d1]
    #                     d1_in = d1_key
    #                     d2_key = d2_keys[index_d2]
    #                     d2_in = d2_key
    #
    #                     if d1_key not in self.inverted_idx:
    #                         d1_in = d1_key.lower()
    #                     if d2_key not in self.inverted_idx:
    #                         d2_in = d2_key.lower()
    #
    #                     if d1_key == d2_key:
    #                         # merge posting list
    #                         self.merge_terms_post_dict(new_dict, d1_key, d1, d2)
    #                         self.inverted_idx[d1_in][1] = str(self.counter_of_postings)
    #                         index_d1 += 1
    #                         index_d2 += 1
    #
    #                     elif d1_key < d2_key:
    #                         # insert d1 posting to the new dict
    #                         if d1_key.lower() == d2_key:
    #                             self.merge_terms_post_dict(new_dict, d2_in, d1, d2)
    #                             self.inverted_idx[d2_in][1] = str(self.counter_of_postings)
    #                             index_d1 += 1
    #                             index_d2 += 1
    #                         else:
    #                             new_dict[d1_in] = d1[d1_key]
    #                             self.inverted_idx[d1_in][1] = str(self.counter_of_postings)
    #                             index_d1 += 1
    #                     else:  # d2_key < d1_key:
    #                         # insert d2 posting to the new dict
    #                         if d2_key.lower() == d1_key:
    #                             self.merge_terms_post_dict(new_dict, d1_in, d1, d2)
    #                             self.inverted_idx[d1_in][1] = str(self.counter_of_postings)
    #                             index_d1 += 1
    #                             index_d2 += 1
    #                         else:
    #                             new_dict[d2_in] = d2[d2_key]
    #                             self.inverted_idx[d2_in][1] = str(self.counter_of_postings)
    #                             index_d2 += 1
    #
    #                     # check if length of new disk equals the required value.
    #                     # if so, save it and append it to added_dicts
    #                     if len(new_dict) == self.postingDict_size:
    #                         new_dict = self.save_new_dict(new_dict, added_dicts)
    #
    #                 if index_d1 >= len(d1):
    #                     is_done_1 = True
    #                     index_l_1 += 1
    #                 if index_d2 >= len(d2):
    #                     is_done_2 = True
    #                     index_l_2 += 1
    #
    #             while index_d1 < len(d1):
    #                 d1_key = d1_keys[index_d1]
    #                 d1_in = d1_key
    #                 if d1_key not in self.inverted_idx:
    #                     d1_in = d1_key.lower()
    #                 # add remaining terms to new dict
    #                 new_dict[d1_in] = d1[d1_key]
    #                 self.inverted_idx[d1_in][1] = str(self.counter_of_postings)
    #                 index_d1 += 1
    #                 # check if length of new disk equals the required value.
    #                 # if so, save it
    #                 if len(new_dict) == self.postingDict_size:
    #                     new_dict = self.save_new_dict(new_dict, added_dicts)
    #
    #             if not is_done_1:
    #                 index_l_1 += 1
    #             while index_d2 < len(d2):
    #                 d2_key = d2_keys[index_d2]
    #                 d2_in = d2_key
    #                 if d2_key not in self.inverted_idx:
    #                     d2_in = d2_key.lower()
    #                 new_dict[d2_in] = d2[d2_key]
    #                 self.inverted_idx[d2_in][1] = str(self.counter_of_postings)
    #                 index_d2 += 1
    #                 # add remaining terms to new dict.
    #                 # check if length of new disk equals the required value.
    #                 # if so, save it
    #                 if len(new_dict) == self.postingDict_size:
    #                     new_dict = self.save_new_dict(new_dict, added_dicts)
    #
    #             if not is_done_2:
    #                 index_l_2 += 1
    #
    #             while index_l_1 < len(list_1):
    #                 added_dicts.append(list_1[index_l_1])
    #                 index_l_1 += 1
    #             while index_l_2 < len(list_2):
    #                 added_dicts.append(list_2[index_l_2])
    #                 index_l_2 += 1
    #
    #             # if new dict's length is larger than 0, save it
    #             if len(new_dict) > 0:
    #                 new_dict = self.save_new_dict(new_dict, added_dicts)
    #
    #             new_merged.append(added_dicts)
    #             i = i + 2
    #         return new_merged

    def second(self):
            new_merged = []
            i = 0
            while i < len(self.merged_dicts):
                if i + 1 >= len(self.merged_dicts):
                    new_merged.append(self.merged_dicts[i])
                    return new_merged

                # posting files names will be added to this list
                added_dicts = []

                list_1 = self.merged_dicts[i]
                list_2 = self.merged_dicts[i + 1]

                index_l_1 = 0
                index_l_2 = 0

                is_done_1 = False
                is_done_2 = False

                d1 = utils.load_obj(list_1[index_l_1], '')
                d2 = utils.load_obj(list_2[index_l_2], '')
                index_d1 = 0
                index_d2 = 0

                d1_keys = list(d1.keys())
                d2_keys = list(d2.keys())

                new_dict = OrderedDict()

                while index_l_1 < len(list_1) and index_l_2 < len(list_2):
                    if is_done_1:
                        d1 = utils.load_obj(list_1[index_l_1], '')
                        is_done_1 = False
                        index_d1 = 0
                    if is_done_2:
                        d2 = utils.load_obj(list_2[index_l_2], '')
                        is_done_2 = False
                        index_d2 = 0

                    d1_keys = list(d1.keys())
                    d2_keys = list(d2.keys())

                    d1_key = d1_keys[index_d1]
                    d1_in = d1_key
                    d2_key = d2_keys[index_d2]
                    d2_in = d2_key
                    while index_d1 < len(d1) and index_d2 < len(d2):
                        d1_key = d1_keys[index_d1]
                        d1_in = d1_key
                        d2_key = d2_keys[index_d2]
                        d2_in = d2_key

                        # if d1_key not in self.inverted_idx:
                        #     d1_in = d1_key.lower()
                        # if d2_key not in self.inverted_idx:
                        #     d2_in = d2_key.lower()

                        if d1_key == d2_key:
                            # merge posting list
                            self.merge_terms_post_dict(new_dict, d1_key, d1, d2)
                            self.inverted_idx[d1_in][1] = str(self.counter_of_postings)
                            index_d1 += 1
                            index_d2 += 1

                        elif d1_key < d2_key:
                            # insert d1 posting to the new dict
                            # if d1_key.lower() == d2_key:
                            #     self.merge_terms_post_dict(new_dict, d2_in, d1, d2)
                            #     self.inverted_idx[d2_in][1] = str(self.counter_of_postings)
                            #     index_d1 += 1
                            #     index_d2 += 1
                            # else:
                            new_dict[d1_in] = d1[d1_key]
                            self.inverted_idx[d1_in][1] = str(self.counter_of_postings)
                            index_d1 += 1
                        else:  # d2_key < d1_key:
                            # insert d2 posting to the new dict
                            # if d2_key.lower() == d1_key:
                            #     self.merge_terms_post_dict(new_dict, d1_in, d1, d2)
                            #     self.inverted_idx[d1_in][1] = str(self.counter_of_postings)
                            #     index_d1 += 1
                            #     index_d2 += 1
                            # else:
                            new_dict[d2_in] = d2[d2_key]
                            self.inverted_idx[d2_in][1] = str(self.counter_of_postings)
                            index_d2 += 1

                        # check if length of new disk equals the required value.
                        # if so, save it and append it to added_dicts
                        if len(new_dict) == self.postingDict_size:
                            new_dict = self.save_new_dict(new_dict, added_dicts)

                    if index_d1 >= len(d1):
                        is_done_1 = True
                        index_l_1 += 1
                    if index_d2 >= len(d2):
                        is_done_2 = True
                        index_l_2 += 1

                while index_d1 < len(d1):
                    d1_key = d1_keys[index_d1]
                    d1_in = d1_key
                    # if d1_key not in self.inverted_idx:
                    #     d1_in = d1_key.lower()
                    # add remaining terms to new dict
                    new_dict[d1_in] = d1[d1_key]
                    self.inverted_idx[d1_in][1] = str(self.counter_of_postings)
                    index_d1 += 1
                    # check if length of new disk equals the required value.
                    # if so, save it
                    if len(new_dict) == self.postingDict_size:
                        new_dict = self.save_new_dict(new_dict, added_dicts)

                if not is_done_1:
                    index_l_1 += 1
                while index_d2 < len(d2):
                    d2_key = d2_keys[index_d2]
                    d2_in = d2_key
                    # if d2_key not in self.inverted_idx:
                    #     d2_in = d2_key.lower()
                    new_dict[d2_in] = d2[d2_key]
                    self.inverted_idx[d2_in][1] = str(self.counter_of_postings)
                    index_d2 += 1
                    # add remaining terms to new dict.
                    # check if length of new disk equals the required value.
                    # if so, save it
                    if len(new_dict) == self.postingDict_size:
                        new_dict = self.save_new_dict(new_dict, added_dicts)

                if not is_done_2:
                    index_l_2 += 1

                while index_l_1 < len(list_1):
                    added_dicts.append(list_1[index_l_1])
                    index_l_1 += 1
                while index_l_2 < len(list_2):
                    added_dicts.append(list_2[index_l_2])
                    index_l_2 += 1

                # if new dict's length is larger than 0, save it
                if len(new_dict) > 0:
                    new_dict = self.save_new_dict(new_dict, added_dicts)

                new_merged.append(added_dicts)
                i = i + 2
            return new_merged

    def switch_to_capitals(self):
        for posting in self.merged_dicts[0]:
            posting_file = utils.load_obj(posting, '')
            new_posting = {}
            changed = False
            for term in posting_file:
                if term in self.entities_dict and self.entities_dict[term] < 2:
                    changed = True
                    del self.inverted_idx[term]
                    continue
                if term in self.global_capitals and self.global_capitals[term]:
                    changed = True
                    # switch in inverted index
                    inverted_val = self.inverted_idx[term]
                    del self.inverted_idx[term]
                    self.inverted_idx[term.upper()] = inverted_val
                    # switch in current posting file
                    posting_val = posting_file[term]
                    # del posting_file[term]
                    new_posting[term.upper()] = posting_file[term]
                    # posting_file[term.upper()] = posting_val
                else:
                    new_posting[term] = posting_file[term]
            if changed:
                utils.save_obj(new_posting, posting, '')


    def save_new_dict(self, new_dict, added_dicts):
        utils.save_obj(new_dict, str(self.counter_of_postings), '')
        added_dicts.append(str(self.counter_of_postings))
        self.counter_of_postings += 1
        new_dict = OrderedDict()
        return new_dict

    def merge_terms_post_dict(self, add_to, term, d1, d2):
        if term in d1:
            l1 = d1[term]
        else:
            # l1 = d1[term.upper()]
            l1 = d1[term]
        if term in d2:
            l2 = d2[term]
        else:
            # l2 = d2[term.upper()]
            l2 = d2[term]

        l1_index = 0
        l2_index = 0

        merged_posting = []
        while l1_index < len(l1) and l2_index < len(l2):
            if l1[l1_index][0] > l2[l2_index][0]:
                merged_posting.append(l2[l2_index])
                l2_index += 1
            else:
                merged_posting.append(l1[l1_index])
                l1_index += 1
        while l1_index < len(l1):
            merged_posting.append(l1[l1_index])
            l1_index += 1
        while l2_index < len(l2):
            merged_posting.append(l2[l2_index])
            l2_index += 1

        add_to[term] = merged_posting
