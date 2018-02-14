from pyspark import SparkConf, SparkContext
from math import sqrt
import sys

conf=SparkConf().setMaster("local[*]").setAppName('SimilarMovies')
sc=SparkContext(conf=conf)

def loadMovies():
    movies={}
    with open("../ml-100k/u.item", encoding='ascii', errors='ignore') as f:
        for line in f:
            values=line.split('|')
            movies[int(values[0])]=values[1]
    return movies

def initMap(line):
    values=line.split()
    userID=int(values[0])
    movieID=int(values[1])
    rating=float(values[2])
    return ( userID, (movieID,rating) )

def filterDuplicate(value):
    ratings=value[1]
    (movieId1, rating1)=ratings[0]
    (movieId2, rating2)=ratings[1]
    return movieId1 < movieId2
    
def newMap(value):
    ratings=value[1]
    (movieId1, rating1)=ratings[0]
    (movieId2, rating2)=ratings[1]
    return ( (movieId1,movieId2),(rating1,rating2) )

def findSimilarity(values):
    numPairs=0
    sum_xx = sum_yy = sum_xy = 0
    for ratingX, ratingY in values:
        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        numPairs += 1
    
    N= sum_xy
    D= sqrt(sum_xx) * sqrt(sum_yy)
    score=0    
    if (D):
        score= (N /float(D))
    return (score,numPairs)
    
def filterSelect(PairSim):
    return ( (PairSim[0][0]==movieID or PairSim[0][1]==movieID) and \
             (PairSim[1][0] > thresholdscore and PairSim[1][1] > cothreshold) \
           )
    
def findSimilarmovies(movieID,similarRatings):
    print('Finding Movies similar to :'+ movie_dict[movieID])

    thr_filter=similarRatings.filter(filterSelect)
#thr_filter.saveAsTextFile("movie-fil")

    similar_movies=thr_filter.sortBy(lambda a: a[1][0],False).take(10)

    for MovRat in similar_movies:
        (movies, scores)=MovRat
        similarID=movies[0]
        if(similarID==movieID):
            similarID=movies[1]
        
        print(movie_dict[similarID]+' score:'+str(scores[0])+' strength:'+str(scores[1]))

    

input_rdd=sc.textFile('../ml-100k/u.data')
#header = input_rdd.first()
#input_rows=input_rdd.filter(lambda line: line != header)
init_maprdd=input_rdd.map(initMap)
self_join=init_maprdd.join(init_maprdd)
unique_self_join=self_join.filter(filterDuplicate)
#unique_self_join.foreach(print)
update_mapped=unique_self_join.map(newMap)
#update_mapped.foreach(print)
grp_rdd=update_mapped.groupByKey()
#grp_rdd.saveAsTextFile("movie-gpr")
similarRatings=grp_rdd.mapValues(findSimilarity).cache()
#similarRatings.saveAsTextFile("movie-simi")
print('Loading Movies.....')
movie_dict=loadMovies()

movieID=int(sys.argv[1])
thresholdscore=0.97
cothreshold=50

findSimilarmovies(movieID,similarRatings)


    
    
    
    

#sort_rdd.saveAsTextFile("movie-sort")








