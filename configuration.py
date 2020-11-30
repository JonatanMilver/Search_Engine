class ConfigClass:
    def __init__(self, corpus_path, output_path, stemming=False):
        self.corpusPath = corpus_path
        # self.corpusPath = "C:\\Users\\yonym\\Desktop\\ThirdYear\\IR\\engineV1\\Data\\date=07-16-2020\\covid19_07-16.snappy.parquet"
        # self.corpusPath = r"C:\Users\Guyza\OneDrive\Desktop\Information_Systems\University\Third_year\Semester_E\Information_Retrieval\Search_Engine_Project\Data\Data\date=07-27-2020\covid19_07-27.snappy.parquet"
        self.savedFileMainFolder = output_path
        self.saveFilesWithStem = self.savedFileMainFolder + "/WithStem"
        self.saveFilesWithoutStem = self.savedFileMainFolder + "/WithoutStem"
        self.toStem = stemming

        print('Project was created successfully..')

    def get__corpusPath(self):
        return self.corpusPath

    def get_out_path(self):
        return self.savedFileMainFolder
