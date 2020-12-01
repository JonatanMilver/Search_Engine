import os
import pandas as pd


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
                for subdir in subdirs:
                    for d, dirs, subfiles in os.walk(os.path.join(dir, subdir)):
                        for file in subfiles:
                            if file.endswith(".parquet"):
                                all_docs.append(os.path.join(subdir, file))
                break

            else:
                for file in files:
                    if file.endswith(".parquet"):
                        all_docs.extend(self.read_file(dir + "\\" + file))

        return all_docs