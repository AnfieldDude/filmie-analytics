import sys
import psycopg2
from REC_modules import *
from random import shuffle
import numpy as np
from movieLikeCorrelationEngine import *


user_id = 3
numGroups = 5

#Get a user's likes. Maybe add dislikes later
userLikeList = grabLikes(user_id)
userDislikeList = grabDisLikes(user_id)


#Create a list with all of the user's likes and dislikes together
usersList = []
usersDisList = []
for i in userLikeList: usersList.append(i[1])
for j in userDislikeList: usersDisList.append(j[1])

#Randomly divide the user's likes into numGroups different groups
shuffle(usersList)
indices = np.arange(len(usersList)//numGroups, len(usersList), len(usersList)//numGroups)
subUsersLists= []
prev = 0
for index in indices:
    subUsersLists.append(usersList[prev:index])
    prev = index
subUsersLists.append(usersList[indices[-1]:])


likeCorrelationEngMatches = 0

for i in range(len(subUsersLists)):
    trainList = list(subUsersLists)
    testList = trainList.pop(i)
    #print(set(np.reshape(trainList, -1)))
    likeCorrelationEngMovies = movieLikeCorrelationEngine(trainList, usersDisList)
    print(likeCorrelationEngMovies)
    print()
    print('testlist')
    print(testList)
    print()
    likeCorrelationEngMatches += len(set(testList)&set(likeCorrelationEngMovies))
    print(likeCorrelationEngMatches)

    
likeCorrelationEngPercent = likeCorrelationEngMatches/len(usersList)

print(len(usersList))
print(likeCorrelationEngPercent)

#for loop through each of the N groups of movies
#during each iteration, remove that group from the total list to create a training group.
#The removed group becomes test group.
#Use the training group as seeds for the rec. engines.
#Test to see if the test group are within outputs from the rec. engines
