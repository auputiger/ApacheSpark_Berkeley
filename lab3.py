import re
DATAFILE_PATTERN = '^(.+),"(.+)",(.*),(.*),(.*)'

def removeQuotes(s):
	""" Remove quotation marks from an input string
	Args:
		s (str): input string that might have the quote "" characters
	Returns:
		str: a string without the quote characters
	"""
	return ''.join(i for i in s if i!='""')
	
def parseDatafileLine(datafileLine):
	""" Parse a line of the data file using the specified regular expression pattern
	Args:
		datafileLine (str): input string that is a line from the data file
	Returns:
		str: a string parsed using the given regular expression and without the quote characters
	"""
	match = re.search(DATAFILE_PATTERN, datafileLine)
	if match is None:
		print 'Invalid datafile line: %s' % datafileLine
		return (datafileLine, -1)
	elif match.group(1) == '"id"':
		print 'Header datafile line: %s' % datafileLine
		return (datafileLine, 0)
	else:
		product = '%s %s %s' % (match.group(2), match.group(3), match.group(4))
		return ((removeQuotes(match.group(1)), product), 1)
		
import sys
import os
from test_helper import Test

baseDir = os.path.join('data')
inputPath = os.path.join('cs100', 'lab3')

GOOGLE_PATH = 'Google.csv'
GOOGLE_SMALL_PATH = 'Google_small.csv'
AMAZON_PATH = 'Amazon.csv'
AMAZON_SMALL_PATH = 'Amazon_small.csv'
GOLD_STANDARD_PATH = 'Amazon_Google_perfectMapping.csv'
STOPWORDS_PATH = 'stopwords.txt'

def parseData(filename):
	""" Parse a data file
	Args:
		filename (str): input file name of the data file
	Returns:
		RDD: a RDD of parsed lines
	"""
	return (sc
			.textFile(filename, 4, 0)
			.map(parseDatafileLine)
			.cache())
			
def loadData(path):
	""" Load a data file
	Args:
		path (str): input file name of the data file
	Returns:
		RDD: a RDD of parsed valid lines
	"""
	filename = os.path.join(baseDir, inputPath, path)
	raw = parseData(filename).cache()
	failed = (raw
			  .filter(lambda s: s[1] == -1)
			  .map(lambda s: s[0])
			  .cache())
	for line in failed.take(10):
		print '%s - Invalid datafile line: %s' %(path, line)
	valid = (raw.
			.filter(lambda s: s[1] == 1)
			.map(lambda s: s[0])
			.cache())
	print '%s - Read %d lines, successfully parsed %d lines, failed to parse %d lines' %(path, raw.count(), valid.count(), failed.count())
	
	assert failed.count() == 0
	assert raw.count() == (valid.count() + 1)
	return valid
	
googleSmall = loadData(GOOGLE_SMALL_PATH)
google = loadData(GOOGLE_PATH)
amazonSmall = loadData(AMAZON_SMALL_PATH)
amazon = loadData(AMAZON_PATH)

# Part 1. Entity Resolution as Text Similarity - Bages of Words
## (1a) Tokenize a String
quickbrownfox = 'a quick brown fox jumps over the lazy dog'
split_regex = r'\W+'

def simpleTokenize(string):
	""" A simple implementation of input string tokenization
	Args:
		string (str): input string
	Returns:
		list: a list of tokens
	"""
	return filter(lambda s: s != '', re.split(split_regex, string.lower()))
	
print simpleTokenize(quickbrownfox)

## (1b) Removing Stopwords
stopfile = os.path.join(baseDir, inputPath, STOPWORDS_PATH)
stopwords = set(sc.textFile(stopfile).collect())
print 'These are the stopwords: %s' % stopwords

def tokenize(string):
	""" An implementation of input string tokenization that excludes stopwords
	Args:
		string (str): input string
	Returns:
		list: a list of tokens without stopwords
	"""
	return filter(lambda s: s != '' and s not in stopwords, re.split(split_regex, string.lower()))
		
print tokenize(quickbrownfox)

## (1c) Tokenizing the small datasets
amazonRecToToken = amazonSmall.map(lambda line: (line[0], tokenize(line[1])))
googleRecToToken = googleSmall.map(lambda line: (line[0], tokenize(line[1])))

def countTokens(vendorRDD):
	""" Count and return the number of tokens
	Args:
		vendorRDD (RDD of (recordId, tokenizedValue)): Pair tuple of record ID to tokenized output
	Returns:
		count: count of all tokens
	"""
	return vendorRDD.flatMap(lambda (recordId, tokenizedValue): tokenizedValue).count()
	
totalTokens = countTokens(amazonRecToToken) + countTokens(googleRecToToken)
print 'There are %s tokens in the combined datasets' % totalTokens

## (1d) Amazon record with the most tokens
def findBiggestRecord(vendorRDD):
	""" Find and return the record with the largest number of tokens
	Args:
		vendorRDD (RDD of (recordId, tokenizedValue)): Pair tuple of record ID to tokenized output
	Returns:
		list: a list of 1 Pair Tuple of record ID and tokens
	"""
	return vendorRDD.sortBy(lambda (recordId, tokenizedValue): -len(tokenizedValue)).take(1)
	
biggestRecordAmazon = findBiggestRecord(amazonRecToToken)
print 'The Amazon record with ID "%s" has the most tokens (%s)' %(biggestRecordAmazon[0][0],
																	len(biggestRecordAmazon[0][1]))
									
# Part 2. Entity Resolution as Text Similarity - Weighted Bag-of-Words using TF-IDF
## (2a) Implement a TF function
def tf(tokens):
	""" Compute TF
	Args:
		tokens (list of str): input list of tokens from tokenize
	Returns:
		dictionary: a dictionary of tokens to its TF values
	"""
	tokenDict = {}
	for t in tokens:
		if t not in tokenDict:
			tokenDict[t] = 1
		else:
			tokenDict[t] += 1
	docLen = len(tokens) * 1.0
	for t in tokenDict.keys():
		tokenDict[t] = tokenDict[t] / docLen
	return tokenDict

print tf(tokenize(quickbrownfox))

## (2b) Create a corpus
corpusRDD = amazonRecToToken.union(googleRecToToken)

## (2c) Implement an IDFs function
corpus = amazonRecToToken.union(googleRecToToken)
N = corpus.map(lambda rec: rec[0]).distinct().count()
print N

def idfs(corpus):
	""" Compute IDF
	Args:
		corpus (RDD): input corpus
	Returns:
		RDD: a RDD of (token, IDF value)
	"""
	N = corpus.map(lambda rec: rec[0]).distinct().count()
	uniqueTokens = corpus.flatMap(lambda rec: list(set(rec[1])))
	tokenCountPairTuple = uniqueTokens.map(lambda token: (token, 1))
	tokenSumPairTuple = tokenCountPairTuple.reduceByKey(lambda a, b: a + b)
	return (tokenSumPairTuple.map(lambda (k, v): (k, 1.0*N/v)))
	
idfsSmall = idfs(amazonRecToToken.union(googleRecToToken))
uniqueTokenCount = idfsSmall.count()

## (2d) Tokens with the smallest IDF
smallIDFTokens = idfsSmall.takeOrdered(11, lambda s: s[1])
print smallIDFTokens

## (2e) IDF Histogram
import matplotlib.pyplot as plt
small_idf_values = idfsSmall.map(lambda s: s[1]).collect()
fig = plt.figure(figsize = (8, 3))
plt.hist(small_idf_values, 50, log = True)
pass

## (2f) Implement a TF-IDF Function
def tfidf(tokens, idfs):
	""" Compute TF-IDF
	Args:
		tokens (list of str): input list of tokens from tokenize
		idfs (dictionary): record to IDF value
	Returns:
		dictionary: a dictionary of records to TF-IDF values
	"""
	tfs = tf(tokens)
	tfIdfDict = {k: tfs[k] * tfs[k] for k in tfs.keys()}
	return tfIdfDict
	
recb000hkgj8k = amazonRecToToken.filter(lambda x: x[0] == 'b000hkgj8k').collect()[0][1]
idfsSmallWeights = idfsSmall.collectAsMap()
rec_b000hkgj8k_weights = tfidf(recb000hkgj8k, idfsSmallWeights)

# Part 3. Entity Resolution as Text Similarity - Cosine Similarity
## (3a) Implement the components of a cosineSimilarity function
import math

def dotprod(a, b):
	""" Compute dot product
	Args:
		a (dictionary): first dictionary of record to value
		b (dictionary): second dictionary of record to value
	Returns:
		dotProd: result of the dot product with the two input dictionaries
	"""
	return sum(a[k]*b[k] for k in a.keys() if b.has_key(k))
	
def norm(a):
	""" Compute square root of the dot product
	Args:
		a (dictionary): a dictionary of record to value
	Returns:
		norm: a dictionary of tokens to its TF values
	"""
	return math.sqrt(sum(a[k]**2 for k in a.keys()))

def cossim(a, b):
	""" Compute cosine similarity
	Args:
		a (dictionary): first dictionary of record to value
		b (dictionary): second dictionary of record to value
	Returns:
		cossim: dot product of two dictionaries divided by the norm of the first dictionary and
                then by the norm of the second dictionary
	"""
	return dotprod(a, b) / (norm(a) * norm(b))
	
## (3c) Perform Entity Resolution
crossSmall = (googleSmall.cartesian(amazonSmall).cache())

similarities = (crossSmall.map(computeSimilarity).cache())

## (3d) Perform Entity Resolution with Broadcast Variables
idfsSmallBroadcast = sc.broadcast(idfsSmallWeights)
similaritiesBroadcast = (crossSmall
                         .map(computeSimilarityBroadcast) #<FILL IN>
                         .cache())

## (3e) Perform a Gold Standard Evaluation

sims = similaritiesBroadcast.map(lambda (googleURL, amazonID, cs): (amazonID+" "+googleURL, cs))

trueDupsRDD = (sims.join(goldStandard))


# Part 4: Scalable Entity Resolution

amazonWeightsRDD = amazonFullRecToToken.map(lambda (recordId, tokenizedValue): (recordId, tfidf(tokenizedValue, idfsFullBroadcast.value))) #<FILL IN>
googleWeightsRDD = googleFullRecToToken.map(lambda rec: (rec[0], tfidf(rec[1], idfsFullBroadcast.value))) #<FILL IN>





