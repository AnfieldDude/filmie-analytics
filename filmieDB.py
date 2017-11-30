import psycopg2
import os


##### Connect to Filmie Database #####
def openDB():

    try :
        conn = psycopg2.connect("dbname=d2kfn1p4pd60om "+
                                "user=rlsnptxjhqpgsc "+
                                "password=6751e8b449b98494a3d6e25c90d4ca5929493c51f09bf93a983e9301ed579107 "+
                                "host=ec2-176-34-186-178.eu-west-1.compute.amazonaws.com port=5432")


        return conn
    
    except :
        print("Connection failed..")
        return None
 
