import json
from collections import Counter
import nltk

nltk.download('averaged_perceptron_tagger')
nltk.download('maxent_ne_chunker')
nltk.download('words')
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize, TweetTokenizer
from nltk import ne_chunk, pos_tag
from document import Document
import re


class Parse:

    def __init__(self):
        self.stop_words = stopwords.words('english')
        self.stop_words.extend(['', ':', '.'])  # should we add www, https?

        self.tweet_tokenizer = TweetTokenizer()

        self.capital_letter_indexer = {}
        self.named_entities = Counter()

    def parse_sentence(self, text):
        """
        This function tokenize, remove stop words and apply lower case for every word within the text
        :param text:
        :return:
        """
        text_tokens = word_tokenize(text)
        # text_tokens = self.tweet_tokenizer.tokenize(text)

        text_tokens_without_stopwords = [w for w in text_tokens if w not in self.stop_words]

        # self.find_named_entities(text_tokens_without_stopwords)

        for idx, token in enumerate(text_tokens_without_stopwords):
            if token[0].isupper():
                if token not in self.capital_letter_indexer.keys():
                    self.capital_letter_indexer[token.lower()] = True
            else:
                self.capital_letter_indexer[token] = False

            token = token.lower()
            text_tokens_without_stopwords[idx] = token

            if token.startswith('#'):
                self.handle_hashtags(text_tokens_without_stopwords, idx)
            elif token == '@':
                self.handle_tags(text_tokens_without_stopwords, idx)
            elif token in ["%", "percent", "percentage"]:
                self.handle_percent(text_tokens_without_stopwords, idx)
            elif token.isnumeric() or "," in token:
                self.handle_number(text_tokens_without_stopwords, idx, token)

        return text_tokens_without_stopwords

    def parse_doc(self, doc_as_list):
        """
        This function takes a tweet document as list and break it into different fields
        :param doc_as_list: list re-preseting the tweet.
        :return: Document object with corresponding fields.
        """
        tweet_id = doc_as_list[0]
        tweet_date = doc_as_list[1]
        full_text = doc_as_list[2]
        url = self.json_convert_string_to_object(doc_as_list[3])
        url_indices = self.json_convert_string_to_object(doc_as_list[4])
        retweet_text = doc_as_list[5]
        retweet_url = self.json_convert_string_to_object(doc_as_list[6])
        retweet_url_indices = self.json_convert_string_to_object(doc_as_list[7])
        quote_text = doc_as_list[8]
        quote_url = self.json_convert_string_to_object(doc_as_list[9])
        quoted_indices = self.json_convert_string_to_object(doc_as_list[10])
        retweet_quoted_text = doc_as_list[11]
        retweet_quoted_url = self.json_convert_string_to_object(doc_as_list[12])
        retweet_quoted_indices = self.json_convert_string_to_object(doc_as_list[13])

        dict_list = [url, retweet_url, quote_url, retweet_quoted_url]
        # holds all URLs in one place
        urls_set = set()
        for d in dict_list:
            if d is not None:
                for key in d.keys():
                    urls_set.add(d[key])

        # removes redundant short URLs from full_text
        full_text = self.clean_text_from_urls(full_text)
        # should we do for quoted and retweet texts as well?


        tokenized_text = self.parse_sentence(full_text)

        self.expand_tokenized_with_url_set(tokenized_text, urls_set)


        term_dict = {}
        doc_length = len(tokenized_text)  # after text operations.

        for term in tokenized_text:
            if term not in term_dict.keys():
                term_dict[term] = 1
            else:
                term_dict[term] += 1

        document = Document(tweet_id, tweet_date, full_text, url, retweet_text, retweet_url, quote_text,
                            quote_url, term_dict, doc_length)
        return document

    def handle_hashtags(self, text_tokens, idx):
        """
        merges text_tokens[idx] with text_tokens[idx+1] such that '#','exampleText' becomes '#exampleText'
        and inserts 'example' and 'Text' to text_tokens
        :param text_tokens: list of all tokens
        :param idx: index of # in text_tokens
        :return:
        """
        if len(text_tokens) > idx + 1:
            text_tokens.extend([x.lower() for x in self.hashtag_split(text_tokens[idx + 1])])
            text_tokens[idx] = (text_tokens[idx] + text_tokens[idx + 1]).lower()
            text_tokens.pop(idx + 1)

    def handle_tags(self, text_tokens, idx):
        """
        merges text_tokens[idx] with text_tokens[idx+1] such that '@','example' becomes '@example'
        :param text_tokens: list of tokenized words
        :param idx: index of @ in text_tokens
        """

        if len(text_tokens) > idx + 1:
            text_tokens[idx] = (text_tokens[idx] + text_tokens[idx + 1]).lower()
            text_tokens.pop(idx + 1)

    def hashtag_split(self, tag):
        """
        splits a multi-word hash-tag to a list of its words
        :param tag: single hash-tag string
        :return: list of words in tag
        """
        return re.findall(r'[a-zA-Z0-9](?:[a-z0-9]+|[A-Z0-9]*(?=[A-Z]|$))', tag)

    def handle_percent(self, text_tokens, idx):
        """
        merges text_tokens[idx] with text_tokens[idx-1] such that "%"/"percent"/"percentage",'50' becomes '50%'
        :param text_tokens: list of tokenized words
        :param idx: index of % in text_tokens
        :return:
        """

        if idx is not 0:
            # if text_tokens[idx - 1].isnumeric():  # what if it is some kind of range?
            number = self.convert_string_to_float(text_tokens[idx - 1])
            if number is not None:  # what if it is some kind of range?
                text_tokens[idx] = text_tokens[idx - 1].lower() + "%"
                text_tokens.pop(idx - 1)

    def handle_number(self, text_tokens, idx, token):
        """
        converts all numbers to single format:
        2 -> 2
        68,800 -> 68.8K
        123,456,678 -> 123.456M
        3.5 Billion -> 3.5B
        :param text_tokens: list of tokenized words
        :param idx: index of % in text_tokens
        :param token: text_tokens[idx]
        :return:
        """
        # if "," in token:
        #     token = token.replace(",", "")
        #     if not token.isnumeric():
        #         return

        number = self.convert_string_to_float(token)
        if number is None:
            return

        # number = float(token)
        multiplier = 1

        if len(text_tokens) > idx + 1:
            if text_tokens[idx + 1] in ["%", "percent", "percentage"]:
                return

            if text_tokens[idx + 1].lower() in ["thousand", "million", "billion"]:
                if text_tokens[idx + 1].lower() == "thousand":
                    multiplier = 1000
                elif text_tokens[idx + 1].lower() == "million":
                    multiplier = 1000000
                elif text_tokens[idx + 1].lower() == "billion":
                    multiplier = 1000000000
                text_tokens.pop(idx + 1)

        number = number * multiplier
        kmb = ""

        if number >= 1000000000:
            number /= 1000000000
            kmb = 'B'

        elif number >= 1000000:
            number /= 1000000
            kmb = 'M'

        elif number >= 1000:
            number /= 1000
            kmb = 'K'

        # if number is not an integer, separates it to integer and fraction
        # and keeps at most the first three digits in the fraction
        if "." in str(number):
            dot_index = str(number).index(".")
            integer = str(number)[:dot_index]
            fraction = str(number)[dot_index:dot_index + 4]

            if fraction == ".0":
                number = integer
            else:
                number = integer + fraction
        else:
            number = str(number)

        text_tokens[idx] = number + kmb

    def convert_string_to_float(self, s):
        """
        tries to convert a string to a float
        if succeeds, returns float
        if fails, returns None
        :param s: string to convert
        :return: float / None
        """
        if "," in s:
            s.replace(",", "")
        try:
            number = float(s)
            return number
        except:
            return None

    def split_url(self, url):
        """
        separates a URL string to its components
        ex:
            url = https://www.instagram.com/p/CD7fAPWs3WM/?igshid=o9kf0ugp1l8x
            output = [https, www.instagram.com, p, CD7fAPWs3WM, igshid, o9kf0ugp1l8x]
        :param url: url as string
        :return: list of sub strings
        """
        r = re.split('[/://?=]', url)
        return [x for x in r if x != '']

    def expand_tokenized_with_url_set(self, to_extend, urls_set):
        """
        extends the to_extend list with the parsed values in url_set
        :param to_extend: list of strings to extend
        :param urls_set: a Set containing URL strings
        :return:
        """
        for url in urls_set:
            to_extend.extend(self.split_url(url))

    # def expand_tokenized_with_url_dict(self, to_extend, url_dict):
    #     """
    #     extends the to_extend list with the components of the values in url_dict
    #     :param to_extend: list to extend
    #     :param url_dict: dictionary containing URL strings as values
    #     :return:
    #     """
    #     try:
    #         if url_dict is None:
    #             return
    #         for v in url_dict.values():
    #             if v is not None:
    #                 to_extend.extend(self.split_url(v))
    #     except:
    #         print(url_dict)

    def json_convert_string_to_object(self, s):
        """
        converts a given string to its corresponding object according to json
        :param s: string to convert
        :return: Object / None
        """
        if s is None:
            return s
        else:
            return json.loads(s)

    # def clean_text(self, text, indices1, indices2, indices3, indices4):
    #     """
    #     for each indices[i], removes all characters in text between indices[i][0] to indices[i][1]
    #     :param text: string to clean
    #     :param indices: list of lists,each sub-list contains starting index and end index at indices[i][0],indices[i][1]
    #     :return: string text
    #     """
    #     indices = []
    #     if indices1 is not None:
    #         indices.extend(indices1)
    #     if indices2 is not None:
    #         indices.extend(indices2)
    #     if indices3 is not None:
    #         indices.extend(indices3)
    #     if indices4 is not None:
    #         indices.extend(indices4)
    #     if indices is None and indices != []:
    #         return text
    #     indices.sort(key=lambda x: x[0], reverse=True)
    #     for indexes in indices:
    #         if text[indexes[0]:indexes[0] + 5] == "https":
    #             text = text[:indexes[0]] + text[indexes[1]:]
    #
    #     return text

    def clean_text_from_urls(self, text):
        """
        removes all URLs from text
        :param text: string
        :return:
        """
        text = re.sub(r'http\S+', '', text)
        # urls = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', full_text)

        return text

    # def use_porter_stemmer(self, list_str):
    #     """
    #     stemms each word in list_str
    #     :param list_str: list of strings
    #     :return: stemmed list of words
    #     """
    #     ps = PorterStemmer()
    #     stemmed_list = []
    #     for w in list_str:
    #         stemmed_list.append(ps.stem(w))
    #
    #     return stemmed_list

    def Find(self, string):
        # findall() has been used
        # with valid conditions for urls in string
        regex = r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))"
        url = re.findall(regex, string)
        return [x[0] for x in url]

    def find_named_entities(self, text_tokens):

        tagged = nltk.pos_tag(text_tokens)
        ne = nltk.ne_chunk(tagged, binary=True)
        words = set()

        for s in ne:
            if isinstance(s, nltk.Tree):
                word = ""
                for i in range(len(s.leaves())):
                    if i == len(s.leaves()) - 1:
                        word += s[i][0]
                    else:
                        word += s[i][0] + " "
                # else:

                word = word.lower()
                words.add(word)

        for w in words:
            self.named_entities[w] += 1

        # print(words)

    # def handle_urls_from_text(self, to_extend, urls, dicts_list):
    #     urls_to_parse = []
    #     dict_keys = set()
    #     for d in dicts_list:
    #         if d is not None:
    #             for key in d.keys():
    #                 dict_keys.add(key)
    #
    #     for url in urls:
    #         if url not in dict_keys:
    #             urls_to_parse.append(url)
    #             print(url)
    #
    #     parsed_urls = [self.split_url(url) for url in urls_to_parse]
    #
    #     to_extend.extend(parsed_urls)

