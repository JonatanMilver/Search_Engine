import time
from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer
from searcher import Searcher
import utils
import numpy as np

from tqdm import tqdm

# one_file = "C:\\Users\\yonym\\Desktop\\ThirdYear\\IR\\engineV1\\Data\\date=07-16-2020\\covid19_07-16.snappy.parquet"
# one_file = "C:\\Users\\Guyza\\OneDrive\\Desktop\\Information_Systems\\University\\Third_year\\Semester_E\\Information_Retrieval\\Search_Engine_Project\\Data\\Data\\date=07-27-2020\\covid19_07-27.snappy.parquet"
# one_file = "C:\\Users\\Guyza\\OneDrive\\Desktop\\Information_Systems\\University\\Third_year\\Semester_E\\Information_Retrieval\\Search_Engine_Project\\Data\\Data\\date=07-21-2020\\covid19_07-21.snappy.parquet"
# one_file = "C:\\Users\\Guyza\\OneDrive\\Desktop\\Information_Systems\\University\\Third_year\\Semester_E\\Information_Retrieval\\Search_Engine_Project\\Data\\Data\\date=07-16-2020\\covid19_07-16.snappy.parquet"

GLOVE_PATH_SERVER = '../../../../glove.twitter.27B.25d.txt'
GLOVE_PATH_LOCAL = 'glove.twitter.27B.25d.txt'
glove_dict = {}


# def load_glove_dict():
#     global glove_dict
with open(GLOVE_PATH_SERVER, 'r', encoding='utf-8') as f:
    for line in tqdm(f):
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
    # try:
    #     documents_list = r.read_file(file_name=config.get__corpusPath())
    # except:
    #     raise Exception("Failed in reading the parquet files")
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

    print('Finished parsing and indexing. Starting to export files')

    # saves last posting file after indexer has done adding documents.
    indexer.save_postings()
    if len(indexer.doc_posting_dict) > 0:
        indexer.save_doc_posting()
    utils.save_dict(indexer.document_dict, "documents_dict", config.get_out_path())

    indexer.delete_dict_after_saving()

    # start = time.time()
    # merges posting files.
    indexer.merge_chunks()
    # print(time.time() - start)
    utils.save_dict(indexer.inverted_idx, "inverted_idx", config.get_out_path())

    # start = time.time()
    # # updates postings and invereted idx with capitals and entities.
    # indexer.switch_to_capitals()
    # print(time.time() - start)

    return number_of_documents, sum_of_doc_lengths/number_of_documents


def load_index(out_path=''):
    print('Load inverted index and document dictionary')
    inverted_index = utils.load_dict("inverted_idx", out_path)
    documents_dict = utils.load_dict("documents_dict", out_path)
    print('Done')
    return inverted_index, documents_dict

  
def search_and_rank_query(query, inverted_index, document_dict, k, num_of_docs, avg_length_per_doc, config):
    p = Parse(config.toStem)

    query_as_list = p.parse_sentence(query)
    searcher = Searcher(inverted_index, document_dict, num_of_docs, avg_length_per_doc, glove_dict, config)
    relevant_docs, query_glove_vec, query_vec = searcher.relevant_docs_from_posting(query_as_list[0])
    ranked_docs = searcher.ranker.rank_relevant_doc(relevant_docs, query_glove_vec, query_vec)
    return searcher.ranker.retrieve_top_k(ranked_docs, k)


# def main1():
#     num_of_docs, avg_length_per_doc = run_engine(corpus_path=None, output_path='', stemming=False)
#     s= time.time()
#     inverted_index, document_dict = load_index()
#     print(time.time() - s)
#     query = input("Please enter a query: ")
#     k = int(input("Please enter number of docs to retrieve: "))
#     tweet_url = 'http://twitter.com/anyuser/status/'
#     start = time.time()
#     # for doc_tuple in search_and_rank_query(query, inverted_index, document_dict, k):
#     for doc_tuple in search_and_rank_query(query, inverted_index, document_dict, k, num_of_docs=num_of_docs, avg_length_per_doc=avg_length_per_doc):
#         print('tweet id: {}, score (unique common words with query): {}'.format(tweet_url+doc_tuple[1], doc_tuple[0]))
#     print(time.time() - start)


def main(corpus_path=None, output_path='', stemming=False, queries=None, num_docs_to_retrieve=1):
    if queries is not None:
        config = ConfigClass(corpus_path, output_path, stemming)
        num_of_docs, avg_length_per_doc = run_engine(config)
        # query_list = queries
        query_list = handle_queries(queries)
        inverted_index, document_dict = load_index(output_path)
        tweet_url = 'http://twitter.com/anyuser/status/'
        for idx, query in enumerate(query_list):
            print("query {}:".format(idx))
            for doc_tuple in search_and_rank_query(query, inverted_index, document_dict, num_docs_to_retrieve, num_of_docs, avg_length_per_doc, config):
                print('\ttweet id: {}, score (unique common words with query): {}'.format(tweet_url+doc_tuple[1], doc_tuple[0]))


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