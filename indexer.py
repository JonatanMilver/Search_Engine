import bisect
from collections import Counter, OrderedDict
import utils
import threading
from concurrent.futures import ThreadPoolExecutor
import numpy as np


class Indexer:
    def __init__(self, config, glove_dict):
        # given a term, returns the number of doc/tweets in which he is in
        # term -> df, posting_idx
        self.inverted_idx = {}
        self.document_dict = {}
        self.locations_at_postings = OrderedDict()
        # posting_list example -> [('banana', [doc1, doc2,..]) ...]
        # doc1 -> ('tweet_id', and more details..)
        self.posting_list = []
        self.posting_dict = {}
        self.postingDict_size = 200000
        self.counter_of_postings = 0
        self.global_capitals = {}
        self.glove_dict = glove_dict
        self.entities_dict = Counter()
        self.config = config

        self.merged_dicts = []
        self.lock = threading.Lock()

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
        document_vec = np.zeros(shape=25)
        for term in document_dictionary:
            if term in self.glove_dict:
                document_vec += self.glove_dict[term]
        document_vec /= len(document_dictionary)
        self.document_dict[document.tweet_id] = (
            document_vec,  # numpy array of size 25 which
            # represents the document in 25 dimensional space(GloVe)
            document.doc_length  # total number of words in tweet
        )
        # Go over each term in the doc
        for term in document_dictionary.keys():
            try:
                if term in self.inverted_idx:
                    if term not in self.posting_dict:
                        self.posting_dict[term] = None
                    self.inverted_idx[term][0] += 1

                else:
                    self.inverted_idx[term] = [1, str(self.counter_of_postings)]
                    self.posting_dict[term] = None

                insert_tuple = (document.tweet_id,  # tweet id
                                # document.doc_length,  # total number of words in tweet
                                document.max_tf,  # number of occurrences of most common term in tweet
                                document.unique_terms,  # number of unique words in tweet
                                len(document_dictionary[term]),  # number of times term is in tweet - tf!
                                # document_vec  # numpy array of size 25 which
                                # represents the document in 25 dimensional space(GloVe)
                                # document_dictionary[term]  # positions of term in tweet
                                )
                # if there're no documents for the current term, insert the first document
                if self.posting_dict[term] is None:
                    self.posting_list.append((term, [insert_tuple]))
                    self.posting_dict[term] = len(self.posting_list) - 1
                else:
                    tuple_idx = self.posting_dict[term]
                    bisect.insort(self.posting_list[tuple_idx][1], insert_tuple)

                # check if posting_dict is full
                if len(self.posting_list) == self.postingDict_size:
                    self.save_postings()

            except:
                print('problem with the following key {}'.format(term))

    def save_postings(self):
        o = sorted(self.posting_list, key=lambda x: x[0])
        self.locations_at_postings[str(self.counter_of_postings)] = utils.save_list(o, str(self.counter_of_postings)
                                                                                    , self.config.get_out_path())
        self.counter_of_postings += 1
        self.posting_dict = {}
        self.posting_list = []

    # def first(self):
    #     while len(self.merged_dicts) > 1:
    #         self.merged_dicts = self.second()
    #
    #
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
    #                     # if d1_key not in self.inverted_idx:
    #                     #     d1_in = d1_key.lower()
    #                     # if d2_key not in self.inverted_idx:
    #                     #     d2_in = d2_key.lower()
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
    #                         # if d1_key.lower() == d2_key:
    #                         #     self.merge_terms_post_dict(new_dict, d2_in, d1, d2)
    #                         #     self.inverted_idx[d2_in][1] = str(self.counter_of_postings)
    #                         #     index_d1 += 1
    #                         #     index_d2 += 1
    #                         # else:
    #                         new_dict[d1_in] = d1[d1_key]
    #                         self.inverted_idx[d1_in][1] = str(self.counter_of_postings)
    #                         index_d1 += 1
    #                     else:  # d2_key < d1_key:
    #                         # insert d2 posting to the new dict
    #                         # if d2_key.lower() == d1_key:
    #                         #     self.merge_terms_post_dict(new_dict, d1_in, d1, d2)
    #                         #     self.inverted_idx[d1_in][1] = str(self.counter_of_postings)
    #                         #     index_d1 += 1
    #                         #     index_d2 += 1
    #                         # else:
    #                         new_dict[d2_in] = d2[d2_key]
    #                         self.inverted_idx[d2_in][1] = str(self.counter_of_postings)
    #                         index_d2 += 1
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
    #                 # if d1_key not in self.inverted_idx:
    #                 #     d1_in = d1_key.lower()
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
    #                 # if d2_key not in self.inverted_idx:
    #                 #     d2_in = d2_key.lower()
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
    #
    # def mergeSortParallel(self, n):
    #     """
    #     Attempt to get parallel mergesort faster in Windows.  There is
    #     something wrong with having one Process instantiate another.
    #     Looking at speedup.py, we get speedup by instantiating all the
    #     processes at the same level.
    #     """
    #     numproc = 300 if n > 300 else n
    #     # instantiate a Pool of workers
    #     pool = ThreadPoolExecutor(max_workers=numproc)
    #     # i.e., perform mergesort on the first 1/numproc of the lyst,
    #     # the second 1/numproc of the lyst, etc.
    #
    #     # Now we have a bunch of sorted sublists.  while there is more than
    #     # one, combine them with merge.
    #     while len(self.merged_dicts) > 1:
    #         to_append = None
    #         if len(self.merged_dicts) % 2 == 1:
    #             to_append = self.merged_dicts.pop()
    #
    #         # get sorted sublist pairs to send to merge
    #         args = [(self.merged_dicts[i], self.merged_dicts[i + 1]) \
    #                 for i in range(0, len(self.merged_dicts), 2)]
    #         self.merged_dicts = list(pool.map(self.mergeWrap, args))
    #         if to_append is not None:
    #             self.merged_dicts.append(to_append)
    #
    #     # Since we start with numproc a power of two, there will always be an
    #     # even number of sorted sublists to pair up, until there is only one.
    #
    #     self.merged_dicts = self.merged_dicts[0]
    #
    # def mergeWrap(self, AandB):
    #     a, b = AandB
    #     return self.merge(a, b)
    #
    # def merge(self, left, right):
    #     """returns a merged and sorted version of the two already-sorted lists."""
    #     ret = []
    #     li = ri = 0
    #
    #     is_done_1 = False
    #     is_done_2 = False
    #     try:
    #
    #         d1 = utils.load_obj(left[li], '')
    #         d2 = utils.load_obj(right[ri], '')
    #     except:
    #         print()
    #
    #     index_d1 = 0
    #     index_d2 = 0
    #
    #     d1_keys = list(d1.keys())
    #     d2_keys = list(d2.keys())
    #
    #     new_dict = OrderedDict()
    #     while li < len(left) and ri < len(right):
    #         if is_done_1:
    #             d1 = utils.load_obj(left[li], '')
    #             is_done_1 = False
    #             index_d1 = 0
    #         if is_done_2:
    #             d2 = utils.load_obj(right[ri], '')
    #             is_done_2 = False
    #             index_d2 = 0
    #
    #         d1_keys = list(d1.keys())
    #         d2_keys = list(d2.keys())
    #
    #         d1_key = d1_keys[index_d1]
    #         d2_key = d2_keys[index_d2]
    #
    #         while index_d1 < len(d1) and index_d2 < len(d2):
    #             d1_key = d1_keys[index_d1]
    #             d2_key = d2_keys[index_d2]
    #
    #             if d1_key == d2_key:
    #                 # merge posting list
    #                 self.merge_terms_post_dict(new_dict, d1_key, d1, d2)
    #                 self.inverted_idx[d1_key][1] = str(self.counter_of_postings)
    #                 index_d1 += 1
    #                 index_d2 += 1
    #
    #             elif d1_key < d2_key:
    #                 new_dict[d1_key] = d1[d1_key]
    #                 self.inverted_idx[d1_key][1] = str(self.counter_of_postings)
    #                 index_d1 += 1
    #             else:  # d2_key < d1_key:
    #                 new_dict[d2_key] = d2[d2_key]
    #                 self.inverted_idx[d2_key][1] = str(self.counter_of_postings)
    #                 index_d2 += 1
    #
    #             # check if length of new disk equals the required value.
    #             # if so, save it and append it to added_dicts
    #             if len(new_dict) == self.postingDict_size:
    #                 new_dict = self.save_new_dict(new_dict, ret)
    #
    #         if index_d1 >= len(d1):
    #             is_done_1 = True
    #             li += 1
    #         if index_d2 >= len(d2):
    #             is_done_2 = True
    #             ri += 1
    #
    #     while index_d1 < len(d1):
    #         d1_key = d1_keys[index_d1]
    #         # add remaining terms to new dict
    #         new_dict[d1_key] = d1[d1_key]
    #         self.inverted_idx[d1_key][1] = str(self.counter_of_postings)
    #         index_d1 += 1
    #         # check if length of new disk equals the required value.
    #         # if so, save it
    #         if len(new_dict) == self.postingDict_size:
    #             new_dict = self.save_new_dict(new_dict, ret)
    #
    #     if not is_done_1:
    #         li += 1
    #
    #     while index_d2 < len(d2):
    #         d2_key = d2_keys[index_d2]
    #         new_dict[d2_key] = d2[d2_key]
    #         self.inverted_idx[d2_key][1] = str(self.counter_of_postings)
    #         index_d2 += 1
    #         # add remaining terms to new dict.
    #         # check if length of new disk equals the required value.
    #         # if so, save it
    #         if len(new_dict) == self.postingDict_size:
    #             new_dict = self.save_new_dict(new_dict, ret)
    #
    #     if not is_done_2:
    #         ri += 1
    #
    #     while li < len(left):
    #         ret.append(left[li])
    #         li += 1
    #     while ri < len(right):
    #         ret.append(right[ri])
    #         ri += 1
    #
    #     # if new dict's length is larger than 0, save it
    #     if len(new_dict) > 0:
    #         new_dict = self.save_new_dict(new_dict, ret)
    #
    #     # new_merged.append(ret)
    #     return ret

    def merge_chunks(self):
        """
        performs a K-way merge on the posting files -> N disk accesses
        :return:
        """
        saved_chunks = []
        chunks_indices = np.zeros(shape=(len(self.locations_at_postings)), dtype=np.int32)
        chunk_length = self.postingDict_size // len(self.locations_at_postings) + 1
        #   inserts the chunks into a chunked list
        for key in self.locations_at_postings:
            loaded, offset = utils.load_list(key, '', self.locations_at_postings[key], chunk_length)
            saved_chunks.append(loaded)
            self.locations_at_postings[key] = offset

        building_list = []
        all_empty = True

        # loops through as long as all postings files didn't finish running.
        while all_empty:
            should_enter = -1

            # loops through as long as one of the chunks is not done.
            while should_enter == -1:
                term_to_enter = self.find_term(saved_chunks, chunks_indices)
                tuples_to_merge = []
                indexes_of_the_indexes_to_increase = []

                # find all tuples that should be merged and the indices should be increased
                for idx, term_idx_in_chunk in enumerate(chunks_indices):
                    if term_idx_in_chunk < len(saved_chunks[idx]) and \
                            saved_chunks[idx][term_idx_in_chunk][0] == term_to_enter:
                        tuples_to_merge.append(saved_chunks[idx][term_idx_in_chunk])
                        indexes_of_the_indexes_to_increase.append(idx)

                merged_tuple = self.merge_terms_into_one(tuples_to_merge)
                building_list.append(merged_tuple)
                self.inverted_idx[merged_tuple[0]][1] = str(self.counter_of_postings)

                # increase the indices that the tuple at the specific location have been inserted to the new posting
                for idx in indexes_of_the_indexes_to_increase:
                    chunks_indices[idx] += 1

                should_enter = self.update_should_enter(saved_chunks, chunks_indices)

                # saving will be here
                if len(building_list) == self.postingDict_size:
                    self.merged_dicts.append(str(self.counter_of_postings))
                    utils.save_list(building_list, str(self.counter_of_postings), '')
                    self.counter_of_postings += 1
                    building_list = []
            # loads new chunks into the save_chunks list in the relevant indices.
            for index in should_enter:
                loaded, offset = utils.load_list(str(index), '',
                                                 self.locations_at_postings[str(index)], chunk_length)
                saved_chunks[index] = loaded
                chunks_indices[index] = 0
                self.locations_at_postings[str(index)] = offset

            # checks whether all postings are done.
            all_empty = False
            for chunk in saved_chunks:
                if len(chunk) > 0:
                    all_empty = True
                    break

        # save of the last posting file.
        if len(building_list) > 0:
            self.merged_dicts.append(str(self.counter_of_postings))
            utils.save_list(building_list, str(self.counter_of_postings), '')

    def merge_terms_into_one(self, tuples_to_merge):
        """
        merges all tuples with the same term into one tuple in order to be appented to the new list.
        :param tuples_to_merge: list of tuples to be merged.
        :return:
        """
        if len(tuples_to_merge) == 1:
            return tuples_to_merge[0]
        ret_tuple = (tuples_to_merge[0][0], [])
        for tup in tuples_to_merge:
            ret_tuple[1].extend(tup[1])
        ret_tuple[1].sort(key=lambda x: x[0])
        return ret_tuple

    def find_term(self, saved_chunks, chunks_indices):
        """
        Find the smallest term, the one that should be appended next.
        :param saved_chunks: list of chunks.
        :param chunks_indices: list of current indices of each chunk.
        :return: the smallest term
        """
        term_to_enter = None
        for idx, chunk in enumerate(saved_chunks):
            if len(chunk) > 0:
                term_to_enter = saved_chunks[idx][chunks_indices[idx]][0]
        for idx in range(len(saved_chunks)):
            if chunks_indices[idx] < len(saved_chunks[idx]) and \
                    saved_chunks[idx][chunks_indices[idx]][0] < term_to_enter:
                term_to_enter = saved_chunks[idx][chunks_indices[idx]][0]
        return term_to_enter

    def update_should_enter(self, saved_chunks, chunks_indices):
        """
        returns -1 if the loop should continue
        otherwise, returns the list of indices of the indices to increase.
        :param saved_chunks: list of chunks.
        :param chunks_indices: list of current indices of each chunk.
        :return:
        """
        load_list = []
        for i in range(len(saved_chunks)):
            if chunks_indices[i] >= len(saved_chunks[i]):
                load_list.append(i)
        if len(load_list) == 0:
            return -1
        return load_list

    def switch_to_capitals(self):
        """
        updating the posting files and the inverted index with terms
        that should be with capital letters and named entities.
        :return:
        """
        # TODO check how we should change the number of maximum threads
        # numproc = 300 if len(self.merged_dicts) > 300 else len(self.merged_dicts)
        numproc = len(self.merged_dicts) // 3 if len(self.merged_dicts) // 3 > 0 else 1
        pool = ThreadPoolExecutor(max_workers=numproc)
        pool.map(self.capital_per_posting, self.merged_dicts)
        self.update_inverted()

        utils.save_dict(self.inverted_idx, "inverted_idx", self.config.get_out_path())

    def capital_per_posting(self, posting):
        """
        updates each posting with capital letters and named entities
        and saves the updated to disk.
        :param posting: posting file loaded from disk.
        :return:
        """
        posting_file = utils.load_list(posting, '', 0)
        new_posting = []
        changed = False
        for term, doc_list in posting_file:
            if term in self.entities_dict and self.entities_dict[term] < 2:
                changed = True
                continue

            if term in self.global_capitals and self.global_capitals[term]:
                changed = True
                new_posting.append((term.upper(), doc_list))

            else:
                new_posting.append((term, doc_list))
        dict_to_save = {}
        if changed:
            for term, doc_list in new_posting:
                dict_to_save[term] = doc_list
        else:
            for term, doc_list in posting_file:
                dict_to_save[term] = doc_list
        utils.save_dict(dict_to_save, posting, '')

    # def save_new_dict(self, new_dict, added_dicts):
    #     self.lock.acquire()
    #     posting_name = str(self.counter_of_postings)
    #     added_dicts.append(posting_name)
    #     self.counter_of_postings += 1
    #     # self.lock.release()
    #     utils.save_list(new_dict, posting_name, '')
    #     # self.lock.acquire()
    #     new_dict = OrderedDict()
    #     self.lock.release()
    #     return new_dict

    # def merge_terms_post_dict(self, add_to, term, d1, d2):
    #     if term in d1:
    #         l1 = d1[term]
    #     else:
    #         # l1 = d1[term.upper()]
    #         l1 = d1[term]
    #     if term in d2:
    #         l2 = d2[term]
    #     else:
    #         # l2 = d2[term.upper()]
    #         l2 = d2[term]
    #
    #     l1_index = 0
    #     l2_index = 0
    #
    #     merged_posting = []
    #     while l1_index < len(l1) and l2_index < len(l2):
    #         if l1[l1_index][0] > l2[l2_index][0]:
    #             merged_posting.append(l2[l2_index])
    #             l2_index += 1
    #         else:
    #             merged_posting.append(l1[l1_index])
    #             l1_index += 1
    #     while l1_index < len(l1):
    #         merged_posting.append(l1[l1_index])
    #         l1_index += 1
    #     while l2_index < len(l2):
    #         merged_posting.append(l2[l2_index])
    #         l2_index += 1
    #
    #     add_to[term] = merged_posting

    def update_inverted(self):
        """
        updates inverted idx dict with words with capitals and named entities.
        :return:
        """
        for term in list(self.inverted_idx):
            if term in self.entities_dict and self.entities_dict[term] < 2:
                del self.inverted_idx[term]
                continue

            if term in self.global_capitals and self.global_capitals[term]:
                inverted_val = self.inverted_idx[term]
                del self.inverted_idx[term]
                self.inverted_idx[term.upper()] = inverted_val
