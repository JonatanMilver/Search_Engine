import search_engine
import os
import numpy as np
from tqdm import tqdm

if __name__ == '__main__':
    # search_engine.main(stemming=True, num_docs_to_retrieve=0, queries=[])
    search_engine.main1()
    # path = 'C:\\Users\\Guyza\\Downloads\\glove.twitter.27B\\glove.twitter.27B.100d.txt'
    # embeddings_dict = {}
    # with open(path, 'r',encoding='utf-8') as f:
    #     for line in tqdm(f):
    #         values = line.split()
    #         word = values[0]
    #         print(word)
    #         vector = np.asarray(values[1:], "float32")
    #         embeddings_dict[word] = vector

