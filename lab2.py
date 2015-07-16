# (1a) Parsing Each Log Line
import re
import datetime

from pyspark.sql import Row

month_map = {'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7,
    'Aug':8,  'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12}
	
def parse_apache_time(s):
	""" Convert Apache time format into a Python datetime object
	Args:
		s (str): date and time in Apache time format
	Returns:
		datetime: datetime object (ignore timezone for now)
	"""
	return datetime.datetime(int(s[7:11]), 
							 month_map[s[3:6]], 
							 int(s[0:2]]), 
							 int(s[12:14]), 
							 int(s[15:17]),
							 int(s[18:20]))
							 
def parseApacheLogLine(logline):
	""" Parse a line in the Apache Common Log Format (CLF)
	Args:
		logline (str): a line of text in the Apache Common Log Format
	Returns:
		tuple: either a dictionary containing the parts of the Apache Access Log and 1, 
			   or the original invalid log line and 0
	"""
	match = re.search(APACHE_ACCESS_LOG_PATTERN, logline)
	if match is None:
		return (logline, 0)
	size_field = match.group(9)
	if size_field == "-":
		size = long(0)
	else:
		size = long(match.group(9))
	return (Row(
		host			= match.group(1), 
		client_identd 	= match.group(2),
		user_id			= match.group(3),
		date_time		= parse_apache_time(match.group(4)),
		method			= match.group(5),
		endpoint		= match.group(6),
		protocol		= match.group(7),
		response_code	= int(match.group(8)),
		content_size	= size
	), 1)
	
APACHE_ACCESS_LOG_PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)" (\d{3}) (\S+)'
	
# (1b) Configuration & Initial RDD Creation
import sys
import os
from test_helper import Test
baseDir = os.path.join('data')
inputPath = os.path.join('cs100', 'lab2', 'apache.access.log.PROJECT')
logFile = os.path.join(baseDir, inputPath)

def parseLogs():
	""" Read and parse log file """
	parsed_logs = (sc
				   .textFile(logFile)
				   .map(parseApacheLogLine)
				   .cache())
	access_logs = (parsed_logs
				   .filter(lambda s: s[1] == 1)
				   .map(lambda s: s[0])
				   .cache())
	failed_logs = (parsed_logs
				   .filter(lambda s: s[1] == 0)
				   .map(lambda s: s[0]))
	failed_logs_count = failed_logs.count()
	if failed_logs_count > 0:
		print 'Number of invalid logline: %d' % failed_logs_count()
		for line in failed_logs.take(20):
			print 'Invalid logline: %s' % line
	
	print 'Read %d lines, successfully parsed %d lines, failed to parse %d lines' % (parsed_logs.count(), access_logs.count(), failed_logs.count())
	return parsed_logs, access_logs, failed_logs
	
parsed_logs, access_logs, failed_logs = parseLogs()
	
# (1c) Data Cleaning
APACHE_ACCESS_LOG_PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)\s*" (\d{3}) (\S+)' #<FILL IN>
parsed_logs, access_logs, failed_logs = parseLogs()

# (2a) Content Size Statistics
content_sizes = access_logs.map(lambda log: log.content_size).cache()
print 'Content Size Avg: %i, Min: %i, Max: %s' %(
	content_sizes.reduce(lambda a, b: a + b) / conten_size.count(), 
	conten_sizes.min(),
	content_sizes.max())
	
# (2b) Response Code Analysis
responseCodeToCount = (access_logs
					   .map(lambda log: (log.response_code, 1))
					   .reduceByKey(lambda a, b: a + b)
					   .cache())
responseCodeToCountList = responseCodeToCount.take(100)
print 'Found %d response codes' % len(responseCodeToCountList)
print 'Response Code Counts: %s' % responseCodeToCountList

# (2c) Response Code Graphing with matplotlib
labels = responseCodeToCount.map(lambda (x, y): x).collect()
print labels
count = access_logs.count()
fracs = responseCodeToCount.map(lambda (x, y): (float(y) / count)).collect()
print fracs

import matlablib.pyplot as plt
def pie_pct_format(value):
	""" Determine the appropriate format string for the pie chart percentage label
	Args:
		value: value of the pie slice
	Returns:
		str: formatted string label; if the slice is too small to fit, returns an empty string for labels
	"""
	return '' if value < 7 else '%.0f%%' % value
fig = plt.figure(figsize = (4.5, 4.5), facecolor = 'white', edgecolor = 'white')
colors = ['yellowgreen', 'lightskyblue', 'gold', 'purple', 'lightcoral', 'yellow', 'black']
explode = (0.05, 0.05, 0.1, 0, 0, 0, 0)
patches, texts, autotexts = plt.pie(fracs, labels=labels, colors=colors,
                                    explode=explode, autopct=pie_pct_format,
                                    shadow=False,  startangle=125)
for text, autotext in zip(texts, autotexts):
    if autotext.get_text() == '':
        text.set_text('')  # If the slice is small to fit, don't show a text label
plt.legend(labels, loc=(0.80, -0.1), shadow=True)
pass

# (2d) Frequent Hosts
hostCountPairTuple = access_logs.map(lambda log: (log.host, 1))
hostSum = hostCountPairTuple.reduceByKey(lambda a, b: a + b)
hostMoreThan10 = hostSum.filter(lambda s: s[1] > 10)
hostPick20 = (hostMoreThan10.map(lambda s: s[0])).take(20)
print 'Any 20 hosts that have accessed more than 10 times: %s' % hostPick20

# (2e) Visualizing Endpoints
endpoints = (access_logs
			 .map(lambda log: (log.endpoint, 1))
			 .reduceByKey(lambda a, b: a + b)
			 .cache())
ends = endpoints.map(lambda (x, y): x).collect()
counts = endpoints.map(lambda (x, y): y).collect()
fig = plt.figure(figsize=(8,4.2), facecolor='white', edgecolor='white')
plt.axis([0, len(ends), 0, max(counts)])
plt.grid(b=True, which='major', axis='y')
plt.xlabel('Endpoints')
plt.ylabel('Number of Hits')
plt.plot(counts)
pass

# (2f) Top Endpoints
endpointCounts = (access_logs
				  .map(lambda log: (log.endpoint, 1))
				  .reduceByKey(lambda a, b: a + b))
topEndpoints = endpointCounts.takeOrdered(10, lambda s: -1 * s[1])
print 'Top 10 Endpoints: %s' % topEndpoints

# (3a) Top 10 Error Endpoints
not200 = access_logs.filter(lambda log: log.response_code != 200) #<FILL IN>

endpointCountPairTuple = not200.map(lambda log: (log.endpoint, 1)) #<FILL IN>

endpointSum = endpointCountPairTuple.reduceByKey(lambda a, b: a + b) #.<FILL IN>

topTenErrURLs = endpointSum.takeOrdered(10, lambda s: -1 * s[1]) # <FILL IN>
print 'Top Ten failed URLs: %s' % topTenErrURLs

# (3b) Number of Unique Hosts
hosts = access_logs.map(lambda log: (log.host, 1)).reduceByKey(lambda a, b: a + b).cache() # <FILL IN>

uniqueHosts = hosts.map(lambda (x, y) : x).collect() # <FILL IN>

uniqueHostCount = len(uniqueHosts) #<FILL IN>
print 'Unique hosts: %d' % uniqueHostCount

# (3c) Number of Unique Daily Hosts
# TODO: Replace <FILL IN> with appropriate code
###### Method 1
#dayToHostPairTuple = access_logs.map(lambda log: ((log.date_time.day, log.host), 1))

#dayGroupedHosts = dayToHostPairTuple.reduceByKey(lambda a, b: a + b).map(lambda (k, v): k)

#dayHostCount = dayGroupedHosts.map(lambda (k, v): (k, 1)).reduceByKey(lambda a, b: a + b)

#dailyHosts = dayHostCount.sortByKey().cache()

#dailyHostsList = dailyHosts.take(30)
#print 'Unique hosts per day: %s' % dailyHostsList

###### Method 2
dayToHostPairTuple = access_logs.map(lambda log : (log.date_time.day,log.host)).distinct()

dayGroupedHosts = dayToHostPairTuple.groupByKey()

dayHostCount = dayGroupedHosts.mapValues(len)

dailyHosts = (dayHostCount.sortByKey()).cache()

dailyHostsList = dailyHosts.take(30)
print 'Unique hosts per day: %s' % dailyHostsList

# (3d) Visualizing the Number of Unique Daily Hosts
daysWithHosts = dailyHosts.map(lambda (k, v): k).collect()
hosts = dailyHosts.map(lambda (k, v): v).collect()

fig = plt.figure(figsize = (8, 4.5), facecolor = 'white', edgecolor = 'white')
plt.axis([min(dayWithHosts), max(dayWithHost), 0, max(hosts)+500])
plt.grid(b = True, which = 'major', axis = 'y')
plt.xlabel('Day')
plt.ylabel('Hosts')
plt.plot(daysWithHosts, hosts)
pass

# (3e) Average Number of Daily Requests per Hosts
dayAndHostTuple = access_logs.map(lambda log : (log.date_time.day,log.host)).distinct()

groupedByDay = dayAndHostTuple.groupByKey()..mapValues(len)

sortedByDday = groupedByDay.sortByKey()

avgDailyReqPerHost = (sortedByDday.join(dailyHosts)
					  .mapValues(lambda v: v[0]/v[1]).sortByKey()
					  ).cache()
					  
# (3f) Visualizing the Average Daily Requests per Unique Host
daysWithAvg = avgDailyReqPerHost.map(lambda (k, v): k).collect()
avgs = avgDailyReqPerHost.map(lambda (k, v): v).collect()

fig = plt.figure(figsize = (8, 4.2), facecolor = 'white', edgecolor = 'white')
plt.axis([0, max(daysWithAvg), 0, ma(avgs)+2])
plt.grid(b = True, which = 'major', axis = 'y')
plt.xlabel('Day')
plt.ylabel('Average')
plt.plot(daysWithAvg, avgs)
pass

# (4a) Counting 404 Response Codes
badRecords = (access_logs.filter(lambda log: log.response_code == 404).cache())

# (4b) Listing 404 Response Code Records
## Method 1
#badEndpoints = badRecords.map(lambda log: log.endpoint) #<FILL IN>
#badUniqueEndpoints = badEndpoints.distinct() #<FILL IN>

## Method 2
badEndpoints = badRecords.map(lambda log: (log.endpoint, 1))
badUniqueEndpoints = badEndpoints.groupByKey().map(lambda (k, v): k)

badUniqueEndpointsPick40 = badUniqueEndpoints.take(40)

# (4c) Listing the Top20 404 Response Code Endpoints
badEndpointsCountPairTuple = badRecords.map(lambda log: (log.endpoint, 1))
badEndpointsSum = badEndpointsCountPairTuple.reduceByKey(lambda a, b: a + b)
badEndpointsTop20 = badEndpointsSum.takeOrdered(20, lambda (k, v): -1 * v)

# (4d) Listing the Top25 404 Response Code Hosts
errHostsCountPairTuple = badRecords.map(lambda log: (log.host, 1))
errHostsSum = errHostsCountPairTuple.reduceByKey(lambda a, b: a + b)
errHostsTop25 = errHostsSum.takeOrdered(25, lambda (k, v): -v)

# (4e) Listing 404 Response Codes per Day
errDateCountPairTuple = badRecords.map(lambda log: (log.date_time.day, 1))
errDateSum = errDateCountPairTuple.reduceByKey(lambda a, b: a + b)
errDateSorted = (errDateSum.sortByKey()).cache()

# (4f) Visualizing the 404 Response Codes by Day
dayWithErrors404 = errDateSorted.map(lambda (k, v): k).collect()
errors404ByDay = errDateSorted.map(lambda (k, v): v).collect()

fig = plt.figure(figsize = (8, 4.2), facecolor = 'white', edgecolor = 'white')
plt.axis([0, max(daysWithErrors404), 0, max(errors404ByDay)])
plt.grid(b = True, which = 'major', axis = 'y')
plt.xlabel('Day')
plt.ylabel('404 Errors')
plt.plot(daysWithErrors404, errors404ByDay)
pass

# (4g) Top 5 Days for 404 Response Codes
topErrDate = errDateSorted.takeOrdered(5, lambda (k, v): -v)

# (4h) Hourly 404 Response Codes
