import pickle
from configuration import ConfigClass
import json
import os


def save_obj(obj, name, path):
    """
    This function save an object as a pickle.
    :param path:
    :param obj: object to save
    :param name: name of the pickle file.
    :return: -
    """
    with open(os.path.join(path, name) + '.pkl', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)
        # json.dump(obj, f)


def load_obj(name, path):
    """
    This function will load a pickle file
    :param path:
    :param name: name of the pickle file
    :return: loaded pickle file
    """
    with open(os.path.join(path, name) + '.pkl', 'rb') as f:
        return pickle.load(f)
        # return json.load(f)
