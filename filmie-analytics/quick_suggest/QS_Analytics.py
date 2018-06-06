import sys
import os
import json
from collections import namedtuple
from datetime import datetime, timedelta
import numpy as np

DIR_PATH = os.path.dirname(os.path.realpath(__file__))

sys.path.insert(0,DIR_PATH + '/../../Filmie-Libraries/inAppFeatures/quickSuggest/bin')
from REC_modules import *


MODEL_PATH = DIR_PATH+"/models/"



# Disable
def blockPrint():
    sys.stdout = open(os.devnull, 'w')

# Restore
def enablePrint():
    sys.stdout = sys.__stdout__


def setupConnection():
    globals()['conn'] = openDB()
    globals()['cur'] = conn.cursor()
    #print("CONNECTION SETUP")


def closeConnection():
    cur.close()
    conn.close()
    #print("CONNECTION CLOSED")


def grabMovieTitles(mList):
    titles = []
    setupConnection()
    cur.execute("SELECT title, id"+
                " FROM movies"+
                " WHERE id = ANY(%s)" +
                ";",(mList,))

    titles = {}
    for i in cur.fetchall(): titles[i[1]]=i[0]
    closeConnection()
    return titles


##################################
##
## GRABS ALL QS ACTIONS
####################################
def grabAllQuickSuggestActions():
    actions = []
    setupConnection()
    cur.execute("SELECT id, command, startlist_id, resultlist_id, created_at, cookie_id"+
                " FROM quicksuggest_action"+
                " ;")
    for i in cur.fetchall(): actions.append(i)
    closeConnection()
    return actions


######################################################################
## GRAB QS ACTIONS WITHIN X HOURS
##
## INPUT: span, integer for number of hours
##
## OUTPUT: actions that occurred from now(t=0) to span(t=-span)
##
######################################################################
def grabQuickSuggestActions(span = 0):
    actions = []
    setupConnection()
    if span== 0:
        cur.execute("SELECT id, command, startlist_id, resultlist_id, created_at, cookie_id"+
                    " FROM quicksuggest_action"+
                    " ;")
    else:
        right_now = datetime.now()
        start_date = right_now + timedelta(hours=-span)
        cur.execute("SELECT id, command, startlist_id, resultlist_id, created_at, cookie_id"+
                    " FROM quicksuggest_action"+
                    " WHERE created_at > '" + start_date.strftime("%Y-%m-%d %H:%M:%S'")+
                    " ;")
    for i in cur.fetchall(): actions.append(i)
    closeConnection()
    return actions


##################################
##
## GRAB MOVIES FROM WATCHLIST(X)
####################################
def grabWatchListMovies(watchlist_id):
    setupConnection()

    cur.execute('SELECT movie_id FROM movie_watchlist WHERE watchlist_id='+str(watchlist_id))
    
    movieList = []
    for item in cur.fetchall():
        movieList.append(item[0])

    closeConnection()
    
    return movieList

##################################
## IN: listA, list of movies in A
##     listB, list of movies in B
##
## OUT: list of movies
## 
## RETURNS THE DIFFERENCE BETWEEN A-or-B and A&B
####################################
def listDelta(listA, listB):
    delta = []
    delta = list((set(listA)|set(listB)) - (set(listA) & set(listB)))
    return delta

'''
x in s	 	test x for membership in s
x not in s	 	test x for non-membership in s
s.issubset(t)	s <= t	test whether every element in s is in t
s.issuperset(t)	s >= t	test whether every element in t is in s
s.union(t)	s | t	new set with elements from both s and t
s.intersection(t)	s & t	new set with elements common to s and t
s.difference(t)	s - t	new set with elements in s but not in t
s.symmetric_difference(t)	s ^ t	new set with elements in either s or t but not 
'''

##################################
## IN: userRatings, list containing userLikes, userDislikes, userNeutrals 
##     
## OUT: BM Chosen
## 
## Re-Calculates which BM was chosen
####################################



##################################
## IN: none
##     
## OUT: centroids, list of lists containing centroid values of UCs
##      movieList, list of movies which correspond to row-movie
## 
## Loads Centroids and MovieList
####################################
def loadCentroidData():
    user_centroid =  loadMatrixFromFile(MODEL_PATH+"centroid.npy")
    user_movieList = loadMatrixFromFile(MODEL_PATH+"centroidMovies.npy").tolist()

    return user_centroid, user_movieList



#################################################################
## IN: list of dicts of every quick suggest action
##
## OUT: altered list of dicts that now holds information if the action is a sub-aciton of an early action and if
##    : the action was followed by a subsequent action
#################################################################
def findChainActions(actionList):

    timeThreshold = 300 #value in second of largest possible time difference between 2 actions

    #################################################################
    #should add logic to test if actions were initiated by same user#
    #################################################################

    #################################################################
    #could also test to see if firstItem's command is subset of secondItem's command
    #################################################################
    
    for i, firstItem in enumerate(actionList):
        for j, secondItem in enumerate(actionList):
            if int(firstItem['id_num']) < int(secondItem['id_num']):

                #check if the first item's result list equal the second item's start list
                #if set(firstItem['resultList']) == set(secondItem['startList']):
                if firstItem['resultlist_id'] == secondItem['startlist_id']:

                    #check and see if the two actions happened close to each other in time
                    timeDiff = datetime.strptime(secondItem['timeStamp'],"%Y-%m-%d %H:%M:%S") - datetime.strptime(firstItem['timeStamp'],"%Y-%m-%d %H:%M:%S")
                    if timeDiff.seconds < timeThreshold:

                        #if everything checks out, set the chainTail for the first item to the id_num of the second item
                        #and the chainHead for the second iteam to the id_num of the first item
                        actionList[i]['chainTail'] = secondItem['id_num']
                        actionList[j]['chainHead'] = firstItem['id_num']


    return actionList                        


##################################
## IN: list of dicts of every quick suggest action
##     
## OUT: original list of dicts is altered and every dict in the list now has a new key
##    : and value that corresponds to the average index position of the movies selected
##    : by the user from the starting list
## 
## Finds the index positions of the movies selected by the user from the starting list and
## and mean averages them. We are trying to see if users are only selecting the first movies
## listed and ignoring those displayed further down the list.
####################################

def findLocationsOfChoices(actionList):

    for i, item in enumerate(actionList):

        #check to see if the current action is a sub-action of a previous action. If so, remove the command list from the previous
        # action from the curent action's command list before calculating the position of the command choices.
        #if len(item['chainHead']) > 0:
        if not not item['chainHead']:
            index = next((k for (k, dummy) in enumerate(actionList) if dummy['id_num'] == item['chainHead']), None)
            commandList = list(set(item['command']) - set(actionList[index]['command']))
        else:
            commandList = item['command']

#        avePos = 0
#        count = 0
#        for spot in commandList:
#            if spot in item['startList']:
#                avePos += item['startList'].index(spot)
#                count += 1
#
#        avePos = avePos/count

        avePos = np.mean([item['startList'].index(spot) for spot in commandList if spot in item['startList']])
        
        actionList[i].update({'aveChoicePos' : avePos})
        
    return actionList




def buildChainList(actionList):

    chainDict = {}
    for item in actionList:
        if not not item['chainTail']:
            if not item['chainHead']:
                chainPosition = 1
                tempList = []
                tempDict = {}
                tempDict['id_num'] = item['id_num']
                tempDict['command_input_list'] = item['command_input_list']
                tempDict['taste'] = item['taste']
                tempDict['keywords'] = item['keywords']
                tempDict['dir_mult'] = item['dir_mult']
                tempDict['writer_mult'] = item['writer_mult']
                tempDict['position'] = chainPosition
                tempDict['startlist_id'] = item['startlist_id']
                tempDict['resultlist_id'] = item['resultlist_id']
                tempDict['listDelta'] = item['listDelta']
                tempDict['cookie_id'] = item['cookie_id']
                tempList.append(tempDict)
                nextIndex = next((k for (k, dummy) in enumerate(actionList) if dummy['id_num'] == item['chainTail']), None)
                while not not actionList[nextIndex]['chainHead']:
                    chainPosition += 1
                    tempDict = {}
                    tempDict['id_num'] = actionList[nextIndex]['id_num']
                    tempDict['command_input_list'] = actionList[nextIndex]['command_input_list']
                    tempDict['taste'] = actionList[nextIndex]['taste']
                    tempDict['keywords'] = actionList[nextIndex]['keywords']
                    tempDict['dir_mult'] = actionList[nextIndex]['dir_mult']
                    tempDict['writer_mult'] = actionList[nextIndex]['writer_mult']
                    tempDict['position'] = chainPosition
                    tempDict['startlist_id'] = actionList[nextIndex]['startlist_id']
                    tempDict['resultlist_id'] = actionList[nextIndex]['resultlist_id']
                    tempDict['listDelta'] = actionList[nextIndex]['listDelta']
                    tempDict['cookie_id'] = actionList[nextIndex]['cookie_id']

                    tempList.append(tempDict)
                    if not not actionList[nextIndex]['chainTail']:
                        nextIndex = next((k for (k, dummy) in enumerate(actionList) if dummy['id_num'] == actionList[nextIndex]['chainTail']), None)
                    else:
                        break

                chainDict.update({'chain_'+str(item['id_num']) : tempList})

        elif not item['chainHead']:
            chainPosition = 1
            tempList = []
            tempDict = {}
            tempDict['id_num'] = item['id_num']
            tempDict['command_input_list'] = item['command_input_list']
            tempDict['taste'] = item['taste']
            tempDict['keywords'] = item['keywords']
            tempDict['dir_mult'] = item['dir_mult']
            tempDict['writer_mult'] = item['writer_mult']
            tempDict['position'] = chainPosition
            tempDict['startlist_id'] = item['startlist_id']
            tempDict['resultlist_id'] = item['resultlist_id']
            tempDict['listDelta'] = item['listDelta']
            tempDict['cookie_id'] = item['cookie_id']
            tempList.append(tempDict)
                    
            chainDict.update({'chain_'+str(item['id_num']) : tempList})

    actionList.append(chainDict)
    return actionList, chainDict

def buildGlob(span = 0):
    glob = []
    if span == 0: actions = grabAllQuickSuggestActions()
    else:         actions = grabQuickSuggestActions(span=span)
    for a in actions:
        action_id, command, startlist_id, resultlist_id, created_at, cookie_id = a
        movies_sid = grabWatchListMovies(startlist_id)
        movies_rid = grabWatchListMovies(resultlist_id)

        delta = listDelta(movies_sid, movies_rid)

        created_at = created_at.strftime("%Y-%m-%d %H:%M:%S")
        
        glob.append([action_id, command, movies_sid, movies_rid, delta, created_at,
                     startlist_id, resultlist_id, cookie_id])
    return glob



def analyzeGlob(glob):

    summary = []
    for item in glob:

        #unpack glob item and put info into dictionary format
        tempDict = {}
        tempDict['id_num'] = item[0]
        #tempDict['command'] = item[1]
        tempDict['startList'] = item[2]
        tempDict['resultList'] = item[3]
        tempDict['listDelta'] = item[4]
        tempDict['timeStamp'] = item[5]
        tempDict['startlist_id'] = item[6]
        tempDict['resultlist_id'] = item[7]
        tempDict['cookie_id'] = item[8]

        #create empty lists that represent chains of quick suggests or 'Going Deeper'
        tempDict['chainHead'] = []
        tempDict['chainTail'] = []

        SWITCHES = ["-taste",
                    "-keywords",
                    "-dir_mult",
                    "-writer_mult"]

        #remove verbose command program and convert the command list from a string to a list
        if item[1].startswith("python3 /app/storage/python/quicksuggestions/quicksuggest.py "):
            commandString = item[1][len("python3 /app/storage/python/quicksuggestions/quicksuggest.py "):]
            commandList = commandString.split()
            print(commandList)
            for s in SWITCHES: tempDict[s[1:]]=False
            tempDict['command_input_list'] = []
            for i in commandList:
                if i in SWITCHES:
                    tempDict[i[1:]] = True
                else:
                    iList = tempDict['command_input_list']
                    iList.append(int(i))
                    tempDict['command_input_list'] = iList
            #tempDict['command'] = [int(s) for s in commandString.split(' ')]
        print(tempDict)
        summary.append(tempDict)


    return summary



##MAIN##

if len(sys.argv)>1: span = int(sys.argv[1])
else: span=0

glob = buildGlob(span=span)

summary = analyzeGlob(glob)

summary = findChainActions(summary)

#summary = findLocationsOfChoices(summary)

summary, chainDict = buildChainList(summary)


#print(json.dumps(summary))
print(json.dumps(chainDict))
