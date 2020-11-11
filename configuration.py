class ConfigClass:
    def __init__(self):
        self.corpusPath = 'C:\\Users\\Guyza\\OneDrive\\Desktop\\Information_Systems\\University\\Third_year\\Semester_E\\Information_Retrieval\\Search_Engine_Project\\Data\\Data\\'
        # self.corpusPath = 'C:\\Users\\Guyza\\OneDrive\\Desktop\\Information_Systems\\University\\Third_year\\Semester_E\\Information_Retrieval\\Search_Engine_Project\\Data_no_folders\\Data'
        self.savedFileMainFolder = ''
        self.saveFilesWithStem = self.savedFileMainFolder + "/WithStem"
        self.saveFilesWithoutStem = self.savedFileMainFolder + "/WithoutStem"
        self.toStem = False

        print('Project was created successfully..')

    def get__corpusPath(self):
        return self.corpusPath
