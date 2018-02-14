

# Degrees of separation (bfs.py)
Implemetation of BFS in apache spark using Python.
Script determines whether two marvel superheroes are connected or not.

##### Input Files
 
 Marvel-graph.txt 
 
 **_Input format_** :   superheroID [List of co-superheroIDs]
 
 ```
 276 2277 5251 4806 2602 2397 612 5016 3499 4401 1809 6312 748 4864 6243 3991 
277 1068 3495 6194 3636 5326 576 4240 710 1472 929 123 403 5059 1361 4366 5525 
3518 5409
 ```
 1. Read file using sc.textFile
 2. Map input line to (superheroID, ([List of co-superheroIDs], distance , color)). 
 Distance=9999 , color=white  as defaults and Distance=0 , color=gray if heroID=startID (to initialise start node with 0 distance and gray color). 
 ```python
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
 ```
 3. Create accumulator so that it can be shared among nodes in order to find connection between heroes(hitCounter>0).
 ```python
 hitCounter=sc.accumulator(0)
 ```
4. Use Flatmap to create new nodes when color=gray and update distance+1 for new nodes. Make visited node black. Update hitCouter if target hero is visited.
```python
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
```
5. Break if hitCounter > 0 which means connections are found between heros.

6. Reduce the RDD created by 4 to get darker color and minimum distance
```python
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
```
7. Repeat 4 to 7
