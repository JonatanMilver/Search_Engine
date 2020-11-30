import os


class ConfigClass:
    def __init__(self, corpus_path, output_path, stemming=False):
        self.corpusPath = corpus_path
        try:
            os.mkdir(output_path)
        except:
            pass
        # self.corpusPath = "C:\\Users\\yonym\\Desktop\\ThirdYear\\IR\\engineV1\\Data\\date=07-16-2020\\covid19_07-16.snappy.parquet"
        self.savedFileMainFolder = output_path
        self.saveFilesWithStem = self.savedFileMainFolder + "/WithStem"
        self.saveFilesWithoutStem = self.savedFileMainFolder + "/WithoutStem"
        self.toStem = stemming


        print('Project was created successfully..')

    def get__corpusPath(self):
        return self.corpusPath

    def get_out_path(self):
        return self.savedFileMainFolder
