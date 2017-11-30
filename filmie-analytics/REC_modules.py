import sys
import os
import psycopg2
from filmieDB import *
import numpy as np
import numpy as array
from operator import itemgetter


def setupConnection():
    globals()['conn'] = openDB()
    globals()['cur'] = conn.cursor()
    #print("CONNECTION SETUP")


def closeConnection():
    cur.close()
    conn.close()
    #print("CONNECTION CLOSED")

## RETURNS LIST OF TUPLES
## ((valueX, old_colIndexX),(valueY, old_colIndexY),(...))
## Assuming Array is Mirrored across Diagonal
def orderColumnFromMatrix(xMatrix, colnum, colIndex, returnSize=0):
    
    if returnSize == 0: returnSize = len(colIndex)
    returnList = []
    
    #BUILD TUPLE LIST
    returnList = list(zip(xMatrix[colnum],colIndex))

    return sorted(returnList, key=itemgetter(0), reverse = True)[:returnSize]


## RETURNS LIST OF TUPLES
## ((valueX, old_colIndexX),(valueY, old_colIndexY),(...))
## Assuming Array is Mirrored across Diagonal
def orderColumn(xCol, colIndex, returnSize=0):
    
    if returnSize == 0: returnSize = len(xCol)
    returnList = []
    
    #BUILD TUPLE LIST
    returnList = list(zip(xCol,colIndex))

    return sorted(returnList, key=itemgetter(0), reverse = True)[:returnSize]


##
def grabAllLikes():
    liked = []
    setupConnection()
    cur.execute("SELECT user_id, movie_id, updated_at"+
                " FROM movie_ratings"+
                " WHERE rating=1"
                " ORDER BY updated_at DESC" +
                ";")

    liked = cur.fetchall()
    closeConnection()
    return liked


def grabAllRated():
    liked = []
    setupConnection()
    cur.execute("SELECT user_id, movie_id, updated_at"+
                " FROM movie_ratings"+
                #" WHERE rating=1"
                " ORDER BY updated_at DESC" +
                ";")

    liked = cur.fetchall()
    closeConnection()
    return liked


def grabMovieTitles(mList):
    titles = []
    setupConnection()
    cur.execute("SELECT title"+
                " FROM movies"+
                " WHERE id = ANY(%s)" +
                ";",(mList,))

    titles = []
    for i in cur.fetchall(): titles.append(i[0])
    closeConnection()
    return titles


#####################################
##   Engine Table
##  1 - Taste Kid Similar Grouping
##  2 - K_Nearest Neighbors
##  3 - Linear Factorization
##
## UPLOADS RECOMMENDATIONS TO DB
## LIST FORMAT = [(movie_id, ranking), ]
#######################################
def uploadrecommendations2DB(user_id, movie_id, ranking, engine_id):
    try:
        setupConnection()
        cur.execute("insert into recommendations ( user_id, movie_id, ranking, engine_id) values ('%d','%d','%d', '%d')" % ( user_id, movie_id, ranking, engine_id))
        conn.commit()
        closeConnection()
        return True
    except:
        print("FAILED TO UPLOAD TO DB")


#############################
## Removes row of user of selected engine_id
#############################
def removeRecs(user_id, engine_id):
    try:
        setupConnection()
        cur.execute("DELETE FROM recommendations WHERE user_id = "+str(user_id)+" AND engine_id = "+str(engine_id)+";")
        conn.commit()
        closeConnection()
        return True
    
    except:
        return False


#############################
## Returns likes from user_id (or list of user_ids)
#############################
def grabLikes(user_id):
    try:
        liked = []
        setupConnection()
        #    for user in userlist:
        if user_id:
            cur.execute("SELECT user_id, movie_id, updated_at"+
                        " FROM movie_ratings"+
                        " WHERE rating=1 AND user_id = " + str(user_id) +
                        " ORDER BY updated_at DESC" +
                        ";")

            liked = cur.fetchall()
        closeConnection()
        return liked
    
    except:
        error_msg = "ERROR IN grabLikes-function"
        return error_msg


def grabDisLikes(user_id):
    try:
        liked = []
        setupConnection()
        #    for user in userlist:
        if user_id:
            cur.execute("SELECT user_id, movie_id, updated_at"+
                        " FROM movie_ratings"+
                        " WHERE rating=-1 AND user_id = " + str(user_id) +
                        " ORDER BY updated_at DESC" +
                        ";")

            liked = cur.fetchall()
        closeConnection()
        return liked
    
    except:
        error_msg = "ERROR IN grabLikes-function"
        return error_msg


    
#####################################################
## Returns likes from user_id (or list of user_ids)
###################################################
def latestLiked(user_id, limit):
    setupConnection()

    ### GRAB USERS MOVIES
    user_filmie_Movies = grabLikes(curFilmie,user_id)
    for item in user_filmie_Movies[:limit]:
        print(item[0])
        
    closeConnection()

##
## INPUT is list of movie ids
## OUTPUT is tuples of (movie_id, genre)
##
def  grabMovieGenre(movieList):
    try:
        genre = []
        setupConnection()
        #    for user in userlist:
        if movieList:
            cur.execute("SELECT movie_id, genre_id"+
                        " FROM genre_movie"+
                        " WHERE movie_id = ANY(%s)" +
                        ";",(movieList,))

            liked_genre = cur.fetchall()
        closeConnection()
        return liked_genre
    except:
        error_msg = "ERROR IN grabMovieGenre-function"
        print(error_msg)
        return False


def genreCountList(xList):
    genreKey = grabGenreTypes()
    count = []
    #BUILD COUNTING LIST 
    for i in range(0, len(genreKey)): count.append(0)

    #INCREMENT by INDEX of GENRE TO COUNT GENRE
    for i in xList: count[i[1]-1] += 1

    genreCount = dict(zip(genreKey, count))
    return genreCount
    
def movieTitlesFilmieDB(movieList):
    setupConnection()
    #    for user in userlist:
    if movieList:
        stmnt = "SELECT title FROM movies WHERE id IN (%s)" % ','.join('%s' for i in movieList)
        cur.execute(stmnt, movieList)
        movieTitles = cur.fetchall()
 
    closeConnection()
    return movieTitles
    
def grabGenreTypes():
    setupConnection()
    cur.execute("SELECT name FROM genres ;")
    genreList = []
    #TURN TUPLE INTO LIST
    for i in cur.fetchall(): genreList.append(i[0])
    closeConnection()
    return genreList



def listGenreHistogram(watchlist_id):
    try:
        #GET MOVIES FROM WATCHLIST
        likesList = grabWatchListMovies(watchlist_id)
        
        #GET THE GENRE(s) OF MOVIES IN LIST
        genreLikeList = grabMovieGenre(likesList)
    
        #COUNT OF EACH GENRE
        genreCount = genreCountList(genreLikeList)

        return normalizeHistogram(genreCount)
    
    except:
        print("FAILED ON LIST :", watchlist_id)



def userGenreHistogram(user_id):
    try:
        userLikes = grabLikes(user_id)
        likesList = []
        # MAKE LIST OF ONLY LIKES

        for i in userLikes: likesList.append(i[1]) 
    
        #GET THE GENRE(s) OF MOVIES IN LIST
        genreLikeList = grabMovieGenre(likesList)
    
        #COUNT OF EACH GENRE
        genreCount = genreCountList(genreLikeList)

        return normalizeHistogram(genreCount)
    
    except:
        print("FAILED ON USER :", user_id)

def normalizeHistogram(xDict):
    totLikes = sum(xDict.values())
    newDict = {}
    for item in xDict.keys():
        newDict[item] = xDict[item]/totLikes


    return newDict

##########################
##  RETURN's WATCHLIST's TITLE & DESCRIPTION
#############################
def grabListInfo(watchlist_id):
    setupConnection()
    
    cur.execute('SELECT title, description FROM watchlists WHERE id='+str(watchlist_id))
    

    title, desc = cur.fetchall()[0]

    closeConnection()
    
    return title, desc


########################
## Grab User(x)'s Lists and return as a LIST
########################
def grabListsFromUser(user_id):
    setupConnection()
    
    cur.execute('SELECT id FROM watchlists WHERE user_id='+str(user_id))
    
    userwatchlist = []
    for item in cur.fetchall():
        userwatchlist.append(item[0])

    closeConnection()
    
    return userwatchlist

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


############################
## Grab List of Users
##
##
#########################
def grabUserList():
    setupConnection()
    
    cur.execute('SELECT id FROM users')
    
    userlist = []
    for item in cur.fetchall():
        userlist.append(item[0])

    closeConnection()
    
    return userlist


#USING KEYS OF A CALCULATING COSINE OF A & B
def vectorCosineDict(A, B):
    xA = []
    xB = []
    try:
        for key in  A.keys():
            xA.append(A[key])
            xB.append(B[key])

        a = np.array(xA)
        b = np.array(xB)

        return np.dot(a,b)/(np.linalg.norm(a)*np.linalg.norm(b))

    except:
        return 0.00
