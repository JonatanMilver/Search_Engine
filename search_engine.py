from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer
from searcher import Searcher
import utils

from tqdm import tqdm

# one_file = "C:\\Users\\yonym\\Desktop\\ThirdYear\\IR\\engineV1\\Data\\date=08-07-2020\\covid19_08-07.snappy.parquet"
# one_file = "C:\\Users\\Guyza\\OneDrive\\Desktop\\Information_Systems\\University\\Third_year\\Semester_E\\Information_Retrieval\\Search_Engine_Project\\Data\\Data\\date=07-27-2020\\covid19_07-27.snappy.parquet"
one_file = "C:\\Users\\Guyza\\OneDrive\\Desktop\\Information_Systems\\University\\Third_year\\Semester_E\\Information_Retrieval\\Search_Engine_Project\\Data\\Data\\date=07-21-2020\\covid19_07-21.snappy.parquet"
corpus_path = "C:\\Users\\yonym\\Desktop\\ThirdYear\\IR\\engineV1\\Data\\"


def run_engine(corpus_path=None, output_path='', stemming=False):
    """

    :return:
    """
    number_of_documents = 0

    config = ConfigClass(corpus_path, output_path, stemming)
    r = ReadFile(corpus_path=config.get__corpusPath())
    p = Parse(config.toStem)
    indexer = Indexer(config)

    # documents_list = r.read_file(file_name='sample2.parquet')
    documents_list = r.read_file(file_name=one_file)
    # parquet_documents_list = r.read_folder(config.get__corpusPath())
    # for parquet_file in parquet_documents_list:
    # documents_list = r.read_file(file_name=parquet_file)
    # Iterate over every document in the file
    for idx, document in tqdm(enumerate(documents_list)):
        # parse the document
        parsed_document = p.parse_doc(document)
        number_of_documents += 1
        # index the document data
        indexer.add_new_doc(parsed_document)

    # indexer.remove_one_time_entities()

    print('Finished parsing and indexing. Starting to export files')

    utils.save_obj(indexer.inverted_idx, "inverted_idx", config.get_out_path())
    utils.save_obj(indexer.postingDict, str(indexer.counter_of_postings), config.get_out_path())




def load_index(out_path=''):
    print('Load inverted index')
    inverted_index = utils.load_obj("inverted_idx", out_path)
    return inverted_index


def search_and_rank_query(query, inverted_index, k):
    p = Parse()
    query_as_list = p.parse_sentence(query)
    searcher = Searcher(inverted_index)
    relevant_docs = searcher.relevant_docs_from_posting(query_as_list)
    ranked_docs = searcher.ranker.rank_relevant_doc(relevant_docs)
    return searcher.ranker.retrieve_top_k(ranked_docs, k)


def main1():
    run_engine()
    query = input("Please enter a query: ")
    k = int(input("Please enter number of docs to retrieve: "))
    inverted_index = load_index()
    for doc_tuple in search_and_rank_query(query, inverted_index, k):
        print('tweet id: {}, score (unique common words with query): {}'.format(doc_tuple[0], doc_tuple[1]))


def main(corpus_path=None, output_path=None, stemming=False, queries=[], num_docs_to_retrieve=0):
    run_engine(corpus_path, output_path, stemming)
    query_list = queries
    k = num_docs_to_retrieve
    inverted_index = load_index(output_path)
    for query in query_list:
        for doc_tuple in search_and_rank_query(query, inverted_index, k):
            print('tweet id: {}, score (unique common words with query): {}'.format(doc_tuple[0], doc_tuple[1]))
