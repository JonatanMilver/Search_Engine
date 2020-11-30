import search_engine


if __name__ == '__main__':
    q = r"C:\Users\yonym\Desktop\ThirdYear\IR\engineV1\full_run_data\queries.txt"
    search_engine.main(stemming=False, num_docs_to_retrieve=20,
                       queries=q,
                       corpus_path='C:\\Users\\yonym\\Desktop\\ThirdYear\\IR\\engineV1\\Data\\', output_path='')
    # search_engine.main1()

    # inverted_indx = utils.load_obj("inverted_idx", '')
    #
    # dicr_6 = utils.load_obj("6", '')
    # dicr_7 = utils.load_obj("7", '')
    #
    # print(len(inverted_indx))
    #
    # print(len(dicr_6))
    # print(len(dicr_7))
    #
    # for key in dicr_6:
    #     if key in dicr_7:
    #         print("same key in 6,7 postings: {}".format(key))
    #
    # for key in inverted_indx:
    #     if key not in dicr_6 and key not in dicr_7:
    #         print("key not in neither postings: {}".format(key))
    #
    # # path = 'C:\\Users\\Guyza\\Downloads\\glove.twitter.27B\\glove.twitter.27B.100d.txt'
    # embeddings_dict = {}
    # with open(path, 'r',encoding='utf-8') as f:
    #     for line in tqdm(f):
    #         values = line.split()
    #         word = values[0]
    #         print(word)
    #         vector = np.asarray(values[1:], "float32")
    #         embeddings_dict[word] = vector

