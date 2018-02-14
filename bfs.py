from pyspark import SparkConf, SparkContext


conf=SparkConf().setMaster("local").setAppName('DegreesofSeparation')
sc=SparkContext(conf=conf)


startID=5306
targetID=14

hitCounter=sc.accumulator(0)

#bfs_rdd.foreach(print)




def convertToBFS(line):
    fields=line.split()
    heroID=int(fields[0])
    connections=[]
    for i in fields[1:]:
        connections.append(int(i))
    
    distance=9999
    color='WHITE'
    
    if (heroID==startID):
        distance=0
        color='GRAY'
    return (heroID,(connections,distance,color))

def createinputRDD(infile):
    input_rdd=sc.textFile(infile)
    return input_rdd.map(convertToBFS) 

def bfsmap(node):
#    print(node)
    heroID=node[0]
    data=node[1]
    connections=data[0]
    distance=data[1]
    color=data[2]
    results=[]
    if (color == 'GRAY'):
        for i in connections:
            newheroID=i
            newdist=distance+1
            newcolor='GRAY'
            
            if (targetID == i):
                hitCounter.add(1)
                
            newnode=(newheroID,([],newdist,newcolor))
            results.append(newnode)
        color='BLACK'
    results.append( (heroID,(connections,distance,color)) )
    return results


def bfsReduce(data1,data2):
    edge1=data1[0]
    edge2=data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    color1=data1[2]
    color2=data2[2]
    
    newdistance=9999
    newcolor = color1
    newconnections=[]
    
    if (len(edge1) > 0):
        newconnections.extend(edge1)
    if (len(edge2) > 0):
        newconnections.extend(edge2)
 
    if (distance1 < newdistance):
        newdistance = distance1

    if (distance2 < newdistance):
        newdistance = distance2
           
    
    if (color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK')):
        newcolor = color2

    if (color1 == 'GRAY' and color2 == 'BLACK'):
        newcolor = color2

    if (color2 == 'WHITE' and (color1 == 'GRAY' or color1 == 'BLACK')):
        newcolor = color1

    if (color2 == 'GRAY' and color1 == 'BLACK'):
        newcolor=color1
    
    

    return (newconnections,newdistance,newcolor)
    
    

bfs_rdd=createinputRDD('../Marvel-graph.txt')

for iteration in range(0,10):
    print("Iteration:"+str(iteration))
    itera_rdd=bfs_rdd.flatMap(bfsmap)
    
    print("Processing " + str(itera_rdd.count()) + " values.")
    #calling an action to compute accumulator
    if (hitCounter.value > 0):
        print("Hit the target character! From " + str(hitCounter.value) \
            + " different direction(s).")
        break
    bfs_rdd=itera_rdd.reduceByKey(bfsReduce)
    
     
     



