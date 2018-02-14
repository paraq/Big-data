from pyspark import SparkConf, SparkContext


conf=SparkConf().setMaster("local").setAppName('Popularmovies')
sc=SparkContext(conf=conf)

input_rdd=sc.textFile('../ml-latest-small/ratings.csv')
header = input_rdd.first()
input_rows=input_rdd.filter(lambda line: line != header)
trim=input_rows.map(lambda x: (int(x.split(',')[1]),(1,float(x.split(',')[2]))))
popular=trim.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).map(lambda a: (a[0],(a[1][0],float("{0:.2f}".format(a[1][1]/a[1][0]))))).sortBy(lambda a: a[1][0],False)

#popular.foreach(print)

movies_rdd=sc.textFile('../ml-latest-small/movies.csv')
header_m = movies_rdd.first()
movies_rows=movies_rdd.filter(lambda line: line != header_m).map(lambda x: (int(x.split(',')[0]),x.split(',')[1]))

most_join=popular.join(movies_rows).collect()
#most_join.foreach(print)
#.map(lambda x :(x[1][1], x[1][0]) ).collect()
#after join tuple is (id,(count,name)) => (name,count)


for i in range(0,10):
	print(most_join[i])
