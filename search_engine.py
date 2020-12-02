from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer
from searcher import Searcher
import utils
import numpy as np
import pandas as pd




GLOVE_PATH_SERVER = '../../../../glove.twitter.27B.25d.txt'
GLOVE_PATH_LOCAL = 'glove.twitter.27B.25d.txt'
glove_dict = {}


with open(GLOVE_PATH_SERVER, 'r', encoding='utf-8') as f:
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
        for idx, document in enumerate(documents_list):
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
    if len(indexer.document_posting_covid) > 0:
        indexer.save_doc_covid()

    indexer.delete_dict_after_saving()

    # merges posting files.
    indexer.merge_chunks()
    utils.save_dict(indexer.inverted_idx, "inverted_idx", config.get_out_path())

    dits = {'number_of_documents': number_of_documents, "avg_length_per_doc": sum_of_doc_lengths/number_of_documents }

    utils.save_dict(dits, 'details', config.get_out_path())




def load_index(out_path=''):
    inverted_index = utils.load_dict("inverted_idx", out_path)
    documents_dict = utils.load_dict("documents_dict", out_path)
    dits = utils.load_dict('details', out_path)
    num_of_docs, avg_length_per_doc = dits['number_of_documents'], dits['avg_length_per_doc']
    return inverted_index, documents_dict, num_of_docs, avg_length_per_doc

  
def search_and_rank_query(query, inverted_index, document_dict, k, num_of_docs, avg_length_per_doc, config):
    p = Parse(config.toStem)
    query_as_list = p.parse_sentence(query)
    searcher = Searcher(inverted_index, document_dict, num_of_docs, avg_length_per_doc, glove_dict, config)
    # s = time.time()
    relevant_docs, query_glove_vec, query_vec = searcher.relevant_docs_from_posting(query_as_list[0])
    # print("Time for searcher: {}".format(time.time() - s))
    # s=time.time()
    ranked_docs = searcher.ranker.rank_relevant_doc(relevant_docs, query_glove_vec, query_vec)
    # print("Time for ranker: {}".format(time.time() - s))
    check = searcher.ranker.retrieve_top_k(ranked_docs, k)
    return check


def main(corpus_path=None, output_path='', stemming=False, queries=None, num_docs_to_retrieve=1):
    if queries is not None:
        config = ConfigClass(corpus_path, output_path, stemming)
        run_engine(config)

        query_list = handle_queries(queries)
        inverted_index, document_dict, num_of_docs, avg_length_per_doc = load_index(output_path)
        # tweet_url = 'http://twitter.com/anyuser/status/'
        # num_of_docs = 10000000
        # avg_length_per_doc = 21.5
        for idx, query in enumerate(query_list):
            docs_list = search_and_rank_query(query, inverted_index, document_dict, num_docs_to_retrieve, num_of_docs, avg_length_per_doc, config)
            for doc_tuple in docs_list:
                print('tweet id: {}, score: {}'.format(str(doc_tuple[1]), doc_tuple[0]))


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