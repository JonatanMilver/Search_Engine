import nltk

from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer
from searcher import Searcher
import utils
from nltk import ne_chunk, pos_tag
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool

from tqdm import tqdm

one_file = "C:\\Users\\yonym\\Desktop\\ThirdYear\\IR\\engineV1\\Data\\date=07-11-2020\\covid19_07-11.snappy.parquet"
corpus_path = "C:\\Users\\yonym\\Desktop\\ThirdYear\\IR\\engineV1\\Data\\"


def run_engine():
    """

    :return:
    """
    number_of_documents = 0

    config = ConfigClass()
    r = ReadFile(corpus_path=config.get__corpusPath())
    p = Parse()
    indexer = Indexer(config)

    # documents_list = r.read_file(file_name='sample2.parquet')
    # documents_list = r.read_file(file_name=one_file)
    parquet_documents_list = r.read_folder(corpus_path)
    for parquet_file in parquet_documents_list:
        documents_list = r.read_file(file_name=parquet_file)
        # Iterate over every document in the file
        for idx, document in tqdm(enumerate(documents_list)):
            # parse the document
            parsed_document = p.parse_doc(document)
            number_of_documents += 1
            # index the document data
            indexer.add_new_doc(parsed_document)
    # convert_words(p, indexer)

    print('Finished parsing and indexing. Starting to export files')

    utils.save_obj(indexer.inverted_idx, "inverted_idx")
    utils.save_obj(indexer.postingDict, "posting")


def load_index():
    print('Load inverted index')
    inverted_index = utils.load_obj("inverted_idx")
    return inverted_index


def search_and_rank_query(query, inverted_index, k):
    p = Parse()
    query_as_list = p.parse_sentence(query)
    searcher = Searcher(inverted_index)
    relevant_docs = searcher.relevant_docs_from_posting(query_as_list)
    ranked_docs = searcher.ranker.rank_relevant_doc(relevant_docs)
    return searcher.ranker.retrieve_top_k(ranked_docs, k)

# def main(corpus_path,output_path,stemming,queries,num_docs_to_retrieve):
def main():
    run_engine()
    query = input("Please enter a query: ")
    k = int(input("Please enter number of docs to retrieve: "))
    inverted_index = load_index()
    for doc_tuple in search_and_rank_query(query, inverted_index, k):
        print('tweet id: {}, score (unique common words with query): {}'.format(doc_tuple[0], doc_tuple[1]))


def convert_words(p, indexer):
    parser_dict = p.capital_letter_indexer
    indexer_dict = indexer.inverted_idx  # maybe not on inverted???
    for k, v in parser_dict.items():
        if v:
            val = indexer_dict[k]
            del indexer_dict[k]
            indexer_dict[k.upper()] = val
