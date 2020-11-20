import json
from fractions import Fraction
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from document import Document
import re


from stemmer import Stemmer


class Parse:

    def __init__(self, stemming=False):
        self.stop_words = stopwords.words('english')
        self.stop_words.extend(['', ':', '.', ',', '(', ')', '[', ']', '{', '}', '', '``', 'rt', '“', '’', 'n\'t', '\'s', '\'ve', '\'m'])
        self.text_tokens = None
        self.stemmer = None
        if stemming:
            self.stemmer = Stemmer()
        self.hashtag_split_pattern = re.compile(r'[a-zA-Z0-9](?:[a-z0-9]+|[A-Z0-9]*(?=[A-Z]|$))')
        self.emoji_pattern = re.compile(pattern="["
                                           u"\U0001F600-\U0001F64F"  # emoticons
                                           u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                           u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                           u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                           "]+", flags=re.UNICODE)
        self.left_slash_pattern = re.compile(r'^-?[0-9]+/0*[1-9][0-9]*$')
        self.right_slash_pattern = re.compile(r'^-?[0-9]+\\0*[1-9][0-9]*$')
        self.days_dict = {"Sat": "saturday", "Sun": "sunday", "Mon": "monday", "Tue": "tuesday", "Wed": "wednsday",
                     "Thu": "thursday", "Fri": "friday"}
        self.months_dict = {"Jul": ("july", "07"), "Aug": ("august", "08")}

    def parse_sentence(self, text):
        """
        This function tokenize, remove stop words and apply lower case for every word within the text
        :param text:
        :param capital_letter_indexer: dictionary for words with capital letters
        :param named_entities: dictionary for named entities in doc
        :return:
        """
        self.text_tokens = word_tokenize(text)
        tokenized_list = []
        ne_words = set()
        entity_chunk = ''
        empty_chunk = 0
        capital_letter_indexer = {}
        named_entities = set()
        # right_side_slash_pat = re.compile(r'^-?[0-9]+/0*[1-9][0-9]*$')
        # left_side_slash_pat = re.compile(r'^-?[0-9]+\\0*[1-9][0-9]*$')

        # '–' , '-'
        for idx, token in enumerate(self.text_tokens):
            if self.stemmer is not None:
                token = self.stemmer.stem_term(token)
                self.text_tokens[idx] = token
            # token = self.take_off_emoji(token)
            token = self.take_emoji_off(token) #this one is faster
            self.text_tokens[idx] = token
            if token == '' or token.lower() in self.stop_words:
                continue
            if len(token) > 0 and token[0].isupper():
                # chunks entities together.
                entity_chunk += token + " "
                empty_chunk += 1
                if token not in capital_letter_indexer:
                    capital_letter_indexer[token.lower()] = True
            else:
                capital_letter_indexer[token] = False
                # add entity to the global counter and to the current words set
                if entity_chunk != '':
                    named_entities.add(entity_chunk[:-1])
                    if empty_chunk > 1:
                        ne_words.add(entity_chunk)
                    entity_chunk = ''
                    empty_chunk = 0

            if token == '#':
                self.handle_hashtags(tokenized_list, idx)
            elif token == '@':
                self.handle_tags(tokenized_list, idx)
            elif self.is_fraction(token):
                self.handle_fraction(tokenized_list, token, idx)
            elif token in ["%", "percent", "percentage"]:
                self.handle_percent(tokenized_list, idx)
            elif token.isnumeric() or "," in token:
                self.handle_number(tokenized_list, idx, token)
            elif '-' in token and len(token) > 1:
                self.handle_dashes(tokenized_list, token)
            elif token == 'https' and idx + 2 < len(self.text_tokens):
                # Will enter only if there are no urls in the dictionaries.
                splitted_trl = self.split_url(self.text_tokens[idx + 2])
                tokenized_list.extend(splitted_trl)
                self.text_tokens[idx + 2] = ''
            else:
                tokenized_list.append(token.lower())

        # appends named entities to the tokenized list
        for word in ne_words:
            tokenized_list.append(word[:-1])
        return tokenized_list, capital_letter_indexer, named_entities

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
        # url_indices = self.json_convert_string_to_object(doc_as_list[4])
        retweet_text = doc_as_list[5]
        retweet_url = self.json_convert_string_to_object(doc_as_list[6])
        # retweet_url_indices = self.json_convert_string_to_object(doc_as_list[7])
        quote_text = doc_as_list[8]
        quote_url = self.json_convert_string_to_object(doc_as_list[9])
        # quoted_indices = self.json_convert_string_to_object(doc_as_list[10])
        # retweet_quoted_text = doc_as_list[11]
        retweet_quoted_url = self.json_convert_string_to_object(doc_as_list[12])
        # retweet_quoted_indices = self.json_convert_string_to_object(doc_as_list[13])
        capital_letter_indexer = {}
        named_entities = set()

        dict_list = [url, retweet_url, quote_url, retweet_quoted_url]
        max_tf = 0
        # holds all URLs in one place
        urls_set = set()
        for d in dict_list:
            if d is not None:
                for key in d.keys():
                    if key is not None and d[key] is not None:
                        urls_set.add(d[key])

        if quote_text is not None:
            full_text = full_text + " " + quote_text

        # removes redundant short URLs from full_text
        if len(urls_set) > 0:
            full_text = self.clean_text_from_urls(full_text)
        # self.text_tokens = word_tokenize(full_text)
        tokenized_text, capital_letter_indexer, named_entities = self.parse_sentence(full_text)

        tokenized_text.extend(self.handle_dates(tweet_date))


        # self.expand_tokenized_with_url_set(tokenized_text, urls_set)

        term_dict = {}
        doc_length = len(tokenized_text)  # after text operations.
        for idx, term in enumerate(tokenized_text):
            if term not in term_dict.keys():
                # holding term's locations at current tweet
                term_dict[term] = [idx]

            else:
                term_dict[term].append(idx)
            if len(term_dict[term]) > max_tf:
                max_tf = len(term_dict[term])

        document = Document(tweet_id, tweet_date, full_text, url, retweet_text, retweet_url, quote_text,
                            quote_url, term_dict, doc_length, max_tf, len(term_dict),
                            capital_letter_indexer, named_entities)
        return document

    def handle_hashtags(self, tokenized_list, idx):
        """
        merges text_tokens[idx] with text_tokens[idx+1] such that '#','exampleText' becomes '#exampleText'
        and inserts 'example' and 'Text' to text_tokens
        :param text: list of all tokens
        :param idx: index of # in text_tokens
        :return:
        """
        if len(self.text_tokens) > idx + 1:
            splitted_hashtags = self.hashtag_split(self.text_tokens[idx + 1])
            # text.extend([x.lower() for x in splitted_hashtags])
            # text[idx] = (text[idx] + text[idx + 1]).lower()
            tokenized_list.append((self.text_tokens[idx] + self.text_tokens[idx + 1]).lower())
            tokenized_list.extend([x.lower() for x in splitted_hashtags])
            # text.pop(idx + 1)
            self.text_tokens[idx + 1] = ''

    def handle_tags(self, tokenized_list, idx):
        """
        merges text_tokens[idx] with text_tokens[idx+1] such that '@','example' becomes '@example'
        :param text: list of tokenized words
        :param idx: index of @ in text_tokens
        """

        if len(self.text_tokens) > idx + 1:
            tokenized_list.append((self.text_tokens[idx] + self.text_tokens[idx + 1]).lower())
            # text.pop(idx + 1)
            self.text_tokens[idx + 1] = ''

    def hashtag_split(self, tag):
        """
        splits a multi-word hash-tag to a list of its words
        :param tag: single hash-tag string
        :return: list of words in tag
        """
        return re.findall(self.hashtag_split_pattern, tag)

    def handle_percent(self, tokenized_list, idx):
        """
        merges text_tokens[idx] with text_tokens[idx-1] such that "%"/"percent"/"percentage",'50' becomes '50%'
        :param text: list of tokenized words
        :param idx: index of % in text_tokens
        :return:
        """

        if idx is not 0:
            # if text_tokens[idx - 1].isnumeric():  # what if it is some kind of range?
            dash_idx = self.text_tokens[idx - 1].find('-')
            if self.is_fraction(self.text_tokens[idx - 1]):
                number = self.text_tokens[idx - 1]
            else:
                number = self.convert_string_to_float(self.text_tokens[idx - 1])
            if number is not None:  # what if it is some kind of range?
                tokenized_list.append(self.text_tokens[idx - 1].lower() + "%")
            elif dash_idx != -1:
                left = self.text_tokens[idx - 1][:dash_idx]
                right = self.text_tokens[idx - 1][dash_idx + 1:]
                if left.isnumeric() and right.isnumeric():
                    tokenized_list.append(self.text_tokens[idx - 1].lower() + "%")

    def handle_number(self, tokenized_list, idx, token):
        """
        converts all numbers to single format:
        2 -> 2
        68,800 -> 68.8K
        123,456,678 -> 123.456M
        3.5 Billion -> 3.5B
        :param text: list of tokenized words
        :param idx: index of % in text_tokens
        :param token: text_tokens[idx]
        :param tokenized_list: tokenized_list
        :return:
        """
        number = self.convert_string_to_float(token)
        if number is None:
            tokenized_list.append(token)
            return

        multiplier = 1

        if len(self.text_tokens) > idx + 1:
            if self.text_tokens[idx + 1] in ["%", "percent", "percentage"]:
                return

            if self.text_tokens[idx + 1].lower() in ["thousand", "million", "billion"]:
                if self.text_tokens[idx + 1].lower() == "thousand":
                    multiplier = 1000
                elif self.text_tokens[idx + 1].lower() == "million":
                    multiplier = 1000000
                elif self.text_tokens[idx + 1].lower() == "billion":
                    multiplier = 1000000000
                self.text_tokens[idx + 1] = ''

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

        tokenized_list.append(number + kmb)

    def convert_string_to_float(self, s):
        """
        tries to convert a string to a float
        if succeeds, returns float
        if fails, returns None
        :param s: string to convert
        :return: float / None
        """
        if "," in s:
            s = s.replace(",", "")
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
        if url is not None:
            r = re.split('[/://?=]', url)
            return [x for x in r if (x != '' and x != 'https')]

    def expand_tokenized_with_url_set(self, to_extend, urls_set):
        """
        extends the to_extend list with the parsed values in url_set
        :param to_extend: list of strings to extend
        :param urls_set: a Set containing URL strings
        :return:
        """
        for url in urls_set:
            to_extend.extend(self.split_url(url))

    def take_off_emoji(self, token):
        """
        takes unnecessary emojies off the text.
        :param token: string
        :return: string without the emojies
        """
        if len(token) == 1 and ord(token) > 126:
            return ''
        for idx in range(len(token)):
            if 0 <= ord(token[idx]) < 126:
                token = token[idx:]
                break
        for idx in range(len(token) - 1, 0, -1):
            if 0 <= ord(token[idx]) < 126:
                token = token[:idx + 1]
                break

        return token

    def take_emoji_off(self, token):
        return self.emoji_pattern.sub(r'', token)

    def json_convert_string_to_object(self, s):
        """
        converts a given string to its corresponding object according to json
        :param s: string to convert
        :return: Object / None
        """
        if s is None or s == '{}':
            return None
        else:
            return json.loads(s)

    def clean_text_from_urls(self, text):
        """
        removes all URLs from text
        :param text: string
        :return: string without urls
        """
        text = re.sub(r'http\S+|www.\S+', '', text)
        return text

    def handle_dashes(self, tokenized_list, token):
        """
        Adds token's words separated to the tokenized list.
        e.g: Word-word will be handled as [Word,word, Word-word]
        :param token: String to separate
        :return: None
        """
        dash_idx = token.find('-')
        tokenized_list.append(token.lower())
        tokenized_list.append(token[:dash_idx].lower())
        tokenized_list.append(token[dash_idx + 1:].lower())

    def handle_fraction(self, tokenized_list, token, idx):
        """
        takes care of strings representing fractions
        if there is a number before the fraction, it concats both tokens into one.
        :param text:
        :param tokenized_list:
        :param token:
        :param idx:
        :return:
        """
        slash_idx = token.find('\\')
        if slash_idx != -1:
            token[slash_idx] = '/'
        frac = str(Fraction(token))
        if idx == 0 and frac != token:
            tokenized_list.append(frac)
        else:
            number = self.convert_string_to_float(self.text_tokens[idx - 1])
            if number is not None:
                tokenized_list.append(self.text_tokens[idx - 1] + " " + token)
                self.text_tokens[idx] = ''
            elif token != frac:
                tokenized_list.append(frac)
                tokenized_list.append(token)
            else:
                tokenized_list.append(token)

    def is_fraction(self, token):
        """
        checks whether given token is a fraction.
        :param token: string
        :return: boolean
        """
        # return re.match(r'^-?[0-9]+/0*[1-9][0-9]*$', token) is not None or \
        #        re.match(r'^-?[0-9]+\\0*[1-9][0-9]*$', token) is not None
        return re.match(self.right_slash_pattern, token) is not None or \
               re.match(self.left_slash_pattern, token) is not None

    def handle_dates(self, tweet_date):
        """
        takes tweet's date and parsing it's information into tokenized_list
        :param tweet_date: date in string
        :return: list of parsed information
        """
        splitted_date = tweet_date.split()
        day_txt = self.days_dict[splitted_date[0]]
        day_num = splitted_date[2]
        month_txt, month_num = self.months_dict[splitted_date[1]]
        date_num = day_num + "/" + month_num + "/" + splitted_date[5]
        return [day_txt, month_txt, date_num, splitted_date[3]]


