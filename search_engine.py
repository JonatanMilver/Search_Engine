from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer
from searcher import Searcher
import utils
import numpy as np
import pandas as pd
from tqdm import tqdm



GLOVE_PATH_SERVER = '../../../../glove.twitter.27B.25d.txt'
GLOVE_PATH_LOCAL = 'glove.twitter.27B.25d.txt'
glove_dict = {}


with open(GLOVE_PATH_LOCAL, 'r', encoding='utf-8') as f:
    for line in f:
        values = line.split()
        word = values[0]
        vector = np.asarray(values[1:], "float32")
        glove_dict[word] = vector


# load_glove_dict()


def run_engine(config):
    """

    :return:
    """

    number_of_documents = 0
    sum_of_doc_lengths = 0

    r = ReadFile(corpus_path=config.get__corpusPath())
    p = Parse(config.toStem)
    indexer = Indexer(config, glove_dict)
    # documents_list = r.read_file(file_name=config.get__corpusPath())
    parquet_documents_list = r.read_folder(config.get__corpusPath())
    for parquet_file in parquet_documents_list:
        documents_list = r.read_file(file_name=parquet_file)
        # Iterate over every document in the file
        for idx, document in tqdm(enumerate(documents_list)):
            # parse the document
            parsed_document = p.parse_doc(document)
            if parsed_document is None:
                continue
            number_of_documents += 1
            sum_of_doc_lengths += parsed_document.doc_length
            # index the document data
            indexer.add_new_doc(parsed_document)

    # saves last posting file after indexer has done adding documents.
    indexer.save_postings()
    if len(indexer.doc_posting_dict) > 0:
        indexer.save_doc_posting()
    utils.save_dict(indexer.document_dict, "documents_dict", config.get_out_path())

    indexer.delete_dict_after_saving()

    # merges posting files.
    indexer.merge_chunks()
    utils.save_dict(indexer.inverted_idx, "inverted_idx", config.get_out_path())



    return number_of_documents, sum_of_doc_lengths/number_of_documents


def load_index(out_path=''):
    inverted_index = utils.load_dict("inverted_idx", out_path)
    documents_dict = utils.load_dict("documents_dict", out_path)
    return inverted_index, documents_dict

  
def search_and_rank_query(query, inverted_index, document_dict, k, num_of_docs, avg_length_per_doc, config):
    p = Parse(config.toStem)
    query_as_list = p.parse_sentence(query)
    searcher = Searcher(inverted_index, document_dict, num_of_docs, avg_length_per_doc, glove_dict, config)
    relevant_docs, query_glove_vec, query_vec = searcher.relevant_docs_from_posting(query_as_list[0])
    ranked_docs = searcher.ranker.rank_relevant_doc(relevant_docs, query_glove_vec, query_vec)
    check = searcher.ranker.retrieve_top_k(ranked_docs, k)
    return check


def main(corpus_path=None, output_path='', stemming=False, queries=None, num_docs_to_retrieve=1):
    if queries is not None:
        config = ConfigClass(corpus_path, output_path, stemming)
        num_of_docs, avg_length_per_doc = run_engine(config)


        # query_list = queries
        query_list = handle_queries(queries)
        inverted_index, document_dict = load_index(output_path)
        # tweet_url = 'http://twitter.com/anyuser/status/'
        # count = 1
        big_list = []
        for idx, query in enumerate(query_list):

            docs_list = search_and_rank_query(query, inverted_index, document_dict, num_docs_to_retrieve, num_of_docs, avg_length_per_doc, config)
            for doc_tuple in docs_list:
                print('tweet id: {}, score: {}'.format(str(doc_tuple[1]), doc_tuple[0]))

            for tup in docs_list:
                big_list.append((idx+1, tup[1], tup[0]))
            # count += 1
        write_to_csv(big_list)

def write_to_csv(tuple_list):

    df = pd.DataFrame(tuple_list, columns=['query','tweet_id', 'score'])
    df.to_csv('results.csv')


def handle_queries(queries):
    if type(queries) is list:
        return queries

    q = []
    with open(queries, 'r', encoding='utf-8') as f:
        for line in f:
            if line != '\n':
                # start = line.find('.')
                # q.append(line[start+2:])
                q.append(line)

    return q