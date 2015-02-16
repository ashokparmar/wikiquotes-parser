#!/usr/bin/env python

'Parses and indexes outstanding wikiquote articles'

import common
import itertools 
import hashlib
import pycassa  
import re 
import redis
import sys
import xml.sax.handler
import MySQLdb

BATCH_SIZE = 500 

RE_LETTERS_ONLY = re.compile("[\"'*]*([\w']+)[*;.!?,\"]*")

LETTER = re.compile('\w')

COMMON = set(['', 'the', 'of', 'and', 'to', 'a', 'in', 'for', 'is', 'on', 
              'that', 'by', 'with', 'I', 'or', 'not', 'you', 'be', 'are', 
              'this', 'at', 'is', 'are', 'from', 'your', 'have', 'as', 
              'from', 'all', 'can', 'more', 'has'])

HASH_SIZE = 96 / 4 # 96 bits of hash in nibbles 


class IgnoreErrors(xml.sax.handler.ErrorHandler):
    '''Ignore all ignorable XML parsing errors.''' 
    def error(self, exception):
        pass
    def warning(self, exception):
        pass

class WikiCommentHandler(xml.sax.handler.ContentHandler):
    '''Make XML text segments indexable

    >>> article_text = '<a>Hello<b>World</b></a>'
    >>> handler = WikiCommentHandler()
    >>> xml.sax.parseString(article_text, handler, IgnoreErrors())
    >>> handler["a"]
    Hello
    >>> handler["a-b"]
    World

    Also provides as_dict() method for generating cassandra payloads 
    '''
    def __init__(self, *args, **kwargs):
        xml.sax.handler.ContentHandler.__init__(self, *args, **kwargs)
        self.character_data = []
        self.text_data = {}
        self.parse_level = []

    def characters(self, content):
        self.character_data.append(content)

    def startElement(self, name, huh=None):
        self.character_data = []
        self.parse_level.append(name)

    def endElement(self, name):
        current_list = self.text_data.setdefault('-'.join(self.parse_level), [])
        current_list.append(''.join(self.character_data))
        self.parse_level.pop()

    def __getitem__(self, key):
        return self.text_data[key][0].encode('utf-8')

    '''@staticmethod
    Writes an integer as an 8 letter string.

    >>> as_ident_string(1)
    00000001 
    '''
    @staticmethod
    def as_ident_string(i):
        return "%08d" % int(i)

    '''Returns the current parsed enwikiquote <page> segment as a
    cassandra parsable field. 
    
    Returns:
      dict(text= ...       # Body of the article
           ident = 0000... # Article ID number 
           title = ...     # Article title 
    '''
    def as_dict(self):
	
        return dict(text=self['page-revision-text'],
                    ident = self.as_ident_string(self['page-id']),
                    title = self['page-title'])



def extract_quotes_categories(text):
    #print text
    quotes = {}
    categories = []
    quotes_end = False
    quotes_key = "default"
    quotes[quotes_key] = []
    for line in iter(text.splitlines()):
	#print line
        if line.startswith('[[Category:'):
            categories.append(line.replace("[","").replace("]","").replace("Category:",""))

        if line.startswith('== External links =='):
            quotes_end = True

        if line.startswith('==='):
            quotes_key = line.replace("=","").strip()
            quotes[quotes_key] = []
        if line.startswith('* ') and not quotes_end:
            quotes[quotes_key].append(line.replace("*","").replace("'","").replace("[","").replace("]",""))

    return quotes, ','.join(categories)

def parse_article_mysql(article_text):
    #print "article", article_text
    handler = WikiCommentHandler()
    xml.sax.parseString(article_text, handler, IgnoreErrors())

    data = handler.as_dict()
    page_id = data['ident']
    page_title = data['title']
    quotes, categories = extract_quotes_categories(data['text'])  
    

    #print page_id, page_title, quotes, categories
    db = MySQLdb.connect("172.17.42.1","root","root","wikiquote")
    for quote_group in quotes:
        for quote in quotes[quote_group]:
	    digest = hashlib.md5(page_id+page_title+quote_group+quote+categories).hexdigest()[:HASH_SIZE]
            #print digest, page_id, page_title, quote_group, quote, categories
	    store_in_mysql(db, digest, page_id, page_title, quote_group, quote, categories)
    db.close()
	
def store_in_mysql(db, quote_md5, page_id, page_title, quote_group, quote, categories):
    # prepare a cursor object using cursor() method
    cursor = db.cursor()

    # Prepare SQL query to INSERT a record into the database.
    sql = "INSERT INTO quote (quote_md5, page_id, \
       page_title, quote_group, quote, categories) \
       VALUES ('%s', '%s', '%s', '%s', '%s', '%s' )" % \
       (quote_md5, page_id, page_title, quote_group, quote, categories)
    try:
        # Execute the SQL command
        cursor.execute(sql)
        # Commit your changes in the database
        db.commit()
    except:
        # Rollback in case there is any error
        db.rollback()



def lines_page(input):
	while True:
		line = input.next()
        	yield line 
		if '</page>' in line:
			return 

def create_page_body(input):
	data = '\n'.join(lines_page(input))
	return data 

def split_pages(input):
	for line in input:
		if '<page>' in line:

		# Put the line containing <page> back at the front of the
		# iter before passing to upload_page

			yield create_page_body(itertools.chain(iter(['<page>']), input))
	
def process(input):
	counter = 1
	try:
		for body in split_pages(input):
			#print body
			parse_article_mysql(body)
			print "page ", counter, "processed"
			counter = counter + 1
	except KeyboardInterrupt:
		return        

if __name__ == '__main__':
#    def log(s):
#        sys.stdout.write(s)
#       sys.stdout.flush()
    process(sys.stdin)
    #process_element_mysql(status=log)
