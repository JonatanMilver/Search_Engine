import os
import pandas as pd
import regex as re
import json
from tqdm import tqdm


class ReadFile:
    def __init__(self, corpus_path):
        self.corpus_path = corpus_path

    def read_file(self, file_name):
        """
        This function is reading a parquet file contains several tweets
        The file location is given as a string as an input to this function.
        :param file_name: string - indicates the path to the file we wish to read.
        :return: a dataframe contains tweets.
        """
        full_path = os.path.join(self.corpus_path, file_name)
        df = pd.read_parquet(full_path, engine="pyarrow")

        return df.values.tolist()

    def read_folder(self, folder_path):
        """
        reads in an intire folder of parquet files
        :param folder_path:
        :return: list of lists, each sub-list represents a tweet/document
        """
        all_docs = []

        for dir, subdirs, files in os.walk(folder_path):  # folder_path should be changed to self.corpus_path
            if subdirs:
                for subdir in tqdm(subdirs):
                    for d, dirs, subfiles in os.walk(dir + subdir):
                        for file in subfiles:
                            if file.endswith(".parquet"):
                                # print(dir + subdir + "\\" + file)
                                # all_docs.extend(self.read_file(dir + subdir + "\\" + file))
                                # all_docs.extend(self.read_file(os.path.join(subdir, file)))
                                paths = os.path.join(folder_path, subdir, file)
                                all_docs.append(os.path.join(subdir, file))
                break

            else:
                for file in files:
                    if file.endswith(".parquet"):
                            all_docs.extend(self.read_file(dir + "\\" + file))

        return all_docs

    # def add_cols_to_dataframe(self, df):
    #
    #     # why do we get df['url'] as a string?
    #     # may be should change it, as for now all urls in full text point to the entire df['url']
    #     urls = []
    #     for idx, full_text in enumerate(df['full_text']):
    #         short_urls = self.Find(full_text)
    #         short_to_urls = {}
    #         for i in range(len(short_urls)):
    #             short_to_urls[short_urls[i]] = df['url'][idx]
    #
    #         urls.append(short_to_urls)
    #
    #     df['url'] = urls

    # def Find(self, string):
    #     # findall() has been used
    #     # with valid conditions for urls in string
    #     regex = r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))"
    #     url = re.findall(regex, string)
    #     return [x[0] for x in url]

    # def convert_urls_to_dicts(self, df):
    #     urls_dict_list = []
    #     retweet_urls_dict_list = []
    #     quoted_urls_dict_list = []
    #     retweet_quoted_urls_dict_list = []
    #     for index, row in tqdm(df.iterrows()):
    #         self.append_to(urls_dict_list, row['urls'])
    #         self.append_to(retweet_urls_dict_list, row['retweet_urls'])
    #         self.append_to(quoted_urls_dict_list, row['quote_urls'])
    #         self.append_to(retweet_quoted_urls_dict_list, row['retweet_quoted_urls'])
    #
    #     df['urls'] = urls_dict_list
    #     df['retweet_urls'] = urls_dict_list
    #     df['quote_urls'] = urls_dict_list
    #     df['retweet_quoted_urls'] = urls_dict_list
    #
    # def append_to(self, add_to, s):
    #     if s is None:
    #         add_to.append(s)
    #     else:
    #         add_to.append(json.loads(s))
