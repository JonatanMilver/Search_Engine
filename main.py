import search_engine


if __name__ == '__main__':

    ### guy's main ###
    q = "queries.txt"
    c_path = r'C:\Users\Guyza\OneDrive\Desktop\Information_Systems\University\Third_year\Semester_E' \
             r'\Information_Retrieval\Search_Engine_Project\Data\Data'
    search_engine.main(stemming=False,
                       num_docs_to_retrieve=20,
                       queries=q,
                       corpus_path=c_path,
                       output_path=''
                       )


    ### yoni's main ###
    q = r"C:\Users\yonym\Desktop\ThirdYear\IR\engineV1\full_run_data\queries.txt"
    search_engine.main(stemming=False, num_docs_to_retrieve=20,
                       queries=q,
                       corpus_path='C:\\Users\\yonym\\Desktop\\ThirdYear\\IR\\engineV1\\Data\\',
                       output_path='')

