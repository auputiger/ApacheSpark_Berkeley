numPartitions = 2
rawRatings = sc.textFile(ratingFilename).repartition(numPartitions)
rawMovies = sc.textFile(moviesFilename)

def get_ratings_tuple(entry):
	""" Parse a line in the ratings dataset
	Args:
		entry (str): a line in the ratings dataset in the form of UserID::MovieID::Rating::Timestamp
	Returns:
		tuple: (UserID, MovieID, Rating)
	"""
	items = entry.split(':')
	return int(items[0]), int(items[1]), float(items[2])
	
def get_movie_tuple(entry):
	""" Parse a line in the movies dataset
	Args:
		entry (str): a line in the movies dataset in the form of MovieID::Title::Genres
	Returns:
		tuple: (MovieID, Title)
	"""
	items = entry.split(':')
	return int(items[0]), items[1]
	
ratingsRDD = rawRatings.map(get_ratings_tuple).cache()
moviesRDD = rawMovies.map(get_movie_tuple).cache()

ratingsCount = ratingsRDD.count()
moviesCount = moviesRDD.count()

def sortFunction(tuple):
	""" Construct the sort string (does not perform actual sorting)
	Args:
		tuple: (rating, MovieName)
	Returns:
		sortString: the value to sort with, 'rating MovieName'
	"""
	key = unicode('%.3f' % tuple[0])
	value = tuple[1]
	return (key + ' ' + value)
	
# Part 1: Basic Recommendations
## (1a) Number of Ratings and Average Ratings for a Movie
def getCountsAndAverages(IDandRatingsTuple):
	""" Calculate average rating
	Args:
		IDandRatingsTuple: a string tuple of (MovieID, (Rating1, Rating2, Rating3, ...))
	Returns:
		tuple: a tuple of (MovieID, (number of ratings, averageRating))
	"""
	MovieID = IDandRatingsTuple[0]
	Ratings = IDandRatingsTuple[1]
	Counts = len(Ratings)
	Avgs = sum(Ratings) * 1.0 / Counts
	return (MovieID, (Counts, Avgs))
	
## (1b) Movies with Highest Average Ratings
movieIDsWithRatingsRDD = (ratingsRDD
						  .map(lambda (UserID, MovieID, Rating):(MovieID, Rating))
						  .groupByKey())
print movieIDsWithRatingsRDD.take(3)
# movieIDsWithAvgRatingsRDD: [(2, (332, 3.174698795180723)), (4, (71, 2.676056338028169)), (6, (442, 3.7918552036199094))]
movieNameWithAvgRatingsRDD = (moviesRDD.join(movieIDsWithRatingsRDD)
							  .map(lambda (movieID, (title, (count, avg))): (avg, title, count))))
print movieNameWithAvgRatingsRDD.take(3)

## (1c) Movies with Highest Average Ratings and more than 500 reviews
movieLimitedAndSortedByRatingRDD = (movieNameWithAvgRatingsRDD.filter(lambda s: s[2] > 500)
									.sortBy(sortFunction, False))
print movieLimitedAndSortedByRatingRDD.take(20)

# Part 2. Collaborative Filtering
## (2a) Creating a Training Set
trainingRDD, validationRDD, testRDD = ratingsRDD.randomSplit([6, 2, 2], seed = 0L)

## (2b) Root Mean Square Error (RMSE)
# TODO: Replace <FILL IN> with appropriate code
import math

def computeError(predictedRDD, actualRDD):
    """ Compute the root mean squared error between predicted and actual
    Args:
        predictedRDD: predicted ratings for each movie and each user where each entry is in the form
                      (UserID, MovieID, Rating)
        actualRDD: actual ratings where each entry is in the form (UserID, MovieID, Rating)
    Returns:
        RSME (float): computed RSME value
    """
    # Transform predictedRDD into the tuples of the form ((UserID, MovieID), Rating)
    predictedReformattedRDD = predictedRDD.map(lambda (UserID, MovieID, Rating): ((UserID, MovieID), float(Rating))) #<FILL IN>

    # Transform actualRDD into the tuples of the form ((UserID, MovieID), Rating)
    actualReformattedRDD = actualRDD.map(lambda (UserID, MovieID, Rating): ((UserID, MovieID), float(Rating))) #<FILL IN>

    # Compute the squared error for each matching entry (i.e., the same (User ID, Movie ID) in each
    # RDD) in the reformatted RDDs using RDD transformtions - do not use collect()
    squaredErrorsRDD = (predictedReformattedRDD
                        .join(actualReformattedRDD)
                        #.groupByKey()
                        #.map(lambda s: (s[1][0]-s[1][1])**2)
                        .map(lambda ((UserID, MovieID), (RatingPred, RatingAct)): (RatingPred - RatingAct)**2)
                       )
                        #.<FILL IN>)

    # Compute the total squared error - do not use collect()
    totalError = squaredErrorsRDD.reduce(lambda a, b: a + b) #<FILL IN>

    # Count the number of entries for which you computed the total squared error
    numRatings = squaredErrorsRDD.count() #<FILL IN>

    # Using the total squared error and the number of entries, compute the RSME
    return math.sqrt(totalError / numRatings) #<FILL IN>


# sc.parallelize turns a Python list into a Spark RDD.
testPredicted = sc.parallelize([
    (1, 1, 5),
    (1, 2, 3),
    (1, 3, 4),
    (2, 1, 3),
    (2, 2, 2),
    (2, 3, 4)])
testActual = sc.parallelize([
     (1, 2, 3),
     (1, 3, 5),
     (2, 1, 5),
     (2, 2, 1)])
testPredicted2 = sc.parallelize([
     (2, 2, 5),
     (1, 2, 5)])

testError = computeError(testPredicted, testActual)
print 'Error for test dataset (should be 1.22474487139): %s' % testError

testError2 = computeError(testPredicted2, testActual)
print 'Error for test dataset2 (should be 3.16227766017): %s' % testError2

testError3 = computeError(testActual, testActual)
print 'Error for testActual dataset (should be 0.0): %s' % testError3

## (2c) Using ALS.train()
from pyspark.mllib.recommendation import ALS
validationForPredictRDD = validationRDD.map(lambda (UserID, MovieID, Rating):(UserID, MovieID))

seed = 5L
iterations = 5
regularizationParameter = 0.1
ranks = [4, 8, 12]
errors = [0, 0, 0]
err = 0
tolerance = 0.02

minError = float('inf')
bestRank = -1
bestIteration = -1
for rank in ranks:
	model = ALS.train(trainingRDD, rank, seed = seed, iterations = iterations, lambda_ = regularizationParameter)
	predictedRatingsRDD = model.predictAll(validationForPredictRDD)
	error = computeError(predicedRatingsRDD, validationsRDD)
	errors[err] = error
	err += 1
	print 'For rank %s the RMSE is %s' % (rank, error)
	if error < minError:
		minError = error
		bestRank = rank
print 'The best model was trained with rank %s' % bestRank

## (2d) Testing the Model
myModel = ALS.train(trainingRDD, bestRank, seed = seed, iterations = iterations, lambda_ = regularizationParameter)
testForPredictingRDD = testRDD.map(lambda (UserID, MovieID, Rating): (UserID, MovieID))
predictedTestRDD = myModel.predictAll(testForPredictingRDD)
testRMSE = computeError(testRDD, predictedTestRDD)
print testRMSE

## (2e) Comparing the Model
trainingAvgRating = trainingRDD.map(lambda (UserID, MovieID, Rating): float(Rating)).mean()
print trainingAvgRating
testForAvgRDD = testRDD.map(lambda (UserID, MovieID, Rating): (UserID, MovieID, trainingAvgRating))
testAvgRMSE = computeError(testRDD, testForAvgRDD)
print testAvgRMSE

# Part 3: Predictions for Yourself
