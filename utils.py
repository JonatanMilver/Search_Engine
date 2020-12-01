import pickle
import os

def load_inverted_index(path):
    ret_dict = {}
    with open(os.path.join(path, 'inverted_idx') + '.pkl', 'rb') as f:
        while True:
            try:
                 pair = pickle.load(f)
                 ret_dict[pair[0]] = pair[1][0]
            except:
                return ret_dict

def save_list(obj, name, path):
    """
    This function save an object as a pickle.
    :param path:
    :param obj: object to save
    :param name: name of the pickle file.
    :return: -
    """

    with open(os.path.join(path, name) + '.pkl', 'wb') as f:
        to_be_told = f.tell()
        for pair in obj:
            pickle.dump(pair, f, pickle.HIGHEST_PROTOCOL)
        return to_be_told


def load_list(name, path, offset, chunk_length=0):
    """
    This function will load a pickle file
    :param path:
    :param name: name of the pickle file
    :return: loaded pickle file
    """
    ret = []
    with open(os.path.join(path, name) + '.pkl', 'rb') as f:
        f.seek(offset)
        if chunk_length == 0:
            while True:
                try:
                    ret.append(pickle.load(f))
                except:
                    return ret
        for i in range(chunk_length):
            try:
                ret.append(pickle.load(f))
            except:
                return ret, f.tell()
        return ret, f.tell()


def load_dict(name, path):
    """
    This function will load a pickle file
    :param path:
    :param name: name of the pickle file
    :return: loaded pickle file
    """
    ret_dict = {}
    with open(os.path.join(path, name) + '.pkl', 'rb') as f:
        while True:
            try:
                 pair = pickle.load(f)
                 ret_dict[pair[0]] = pair[1]
            except:
                return ret_dict


def save_dict(obj, name, path):
    """
    This function save an object as a pickle.
    :param path:
    :param obj: object to save
    :param name: name of the pickle file.
    :return: -
    """
    with open(os.path.join(path, name) + '.pkl', 'wb') as f:
        to_be_told = f.tell()
        for pair in obj.items():
            pickle.dump(pair, f, pickle.HIGHEST_PROTOCOL)

