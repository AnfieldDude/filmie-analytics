import psycopg2
from REC_modules import *
from operator import itemgetter


def MovieCosineRelations(movieLikes):

    relationMatrix = np.dot(movieLikes.T, movieLikes)
    movieLikesVectorMag = np.linalg.norm(movieLikes, axis=0)
    relationMatrix = relationMatrix / np.outer(movieLikesVectorMag.T, movieLikesVectorMag)
    return(relationMatrix)


def movieLikeCorrelationEngine(likesList, dislikesList):

    allUsersLikes = grabAllRated()

    # GENERATE MATRIX
    # UNIQUE MOVIE LIST (List => SET => List )
    #THESE ARE THE COLUMN INDEX
    movieList = list(set([x[1] for x in allUsersLikes]))

    # UNIQUE USER LIST (List => SET => List )
    #THESE ARE THE ROW INDEX 
    userList = list(set([x[0] for x in allUsersLikes]))


    ## BUILD MATRIX OF MOVIES VS USERS (Col=Movies, Row=User)
    likeMatrix = np.zeros((len(userList),len(movieList)))

    ## FILL MATRIX WITH VALUES
    for item in allUsersLikes:
        uid = userList.index(item[0])
        mid = movieList.index(item[1])
        likeMatrix[uid,mid] = 1

    boomMatrix = MovieCosineRelations(likeMatrix)
    #print('boom')
    #print(boomMatrix)

    #orderColumn(boomMatrix, COL-SELCTED , movieList, LIMIT)
    movieCorrelation = orderColumnFromMatrix(boomMatrix, 15 , movieList, 11)

    # REMOVE THE Mx:Mx element (always at top)
    # REMEMBER TO ADD 1 to list
    #movieCorrelation.pop(0)


    #USING USER'S LIKES

    # CREATE USER VECTOR OF LIKES BASED ON MOVIES
    userInputVec = np.zeros((len(movieList),1))
    #should be zero

    for i in movieList:
        if i in likesList:
            userInputVec[movieList.index(i),0] = 1

    #NO NEED TO RECREATE MATRIX
    for j in movieList:
        if j in dislikesList:
            userInputVec[movieList.index(j),0] = -1
   # print('userinputvector')
   # print(userInputVec[0:99])

    recList = boomMatrix.dot(userInputVec)       
    #print('reclist')
    #print(recList[0:99])

    movieCorrelation = orderColumn(recList, movieList, 200)
    print(movieCorrelation[0:99])
    movieIDsList =[]

    ## REMOVE ALREADY LIKED ... WE SHOULD ALSO REMOVE DISLIKED WHEN ACCOUNTED FOR
    for i in movieCorrelation:
        if i[1] not in likesList:
            if i[1] not in dislikesList: movieIDsList.append(i[1])

    return(movieIDsList)
