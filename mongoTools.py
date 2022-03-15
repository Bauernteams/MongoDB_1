from typing import Dict
from pymongo import MongoClient
from scipy.io import loadmat
import os
from InFusionTools import checkIfSignalWasUploaded, convertTime
import numpy as np
from multiprocessing import Process, Lock
import certifi
from inspect import currentframe, getframeinfo

frameinfo = getframeinfo(currentframe())

def connectMongoDB(local=False):    
    # connect to MongoDB
    if local:
        client = MongoClient("127.0.0.1", port=27017)
    else:
        ca = certifi.where()
        client = MongoClient("mongodb+srv://Bauernteams:4FCQK4LPBXSY4Ss@cluster0.dazrx.mongodb.net/myFirstDatabase?retryWrites=true&w=majority", tlsCAFile=ca)
    db = client.InFusion # use InFusion database
    return db

def mongoUploadFileMulti(lock, s_filePath, ls_SignalWL=None, ls_MessageWL=None, ls_ChannelWL = None, ls_ChannelBL=None, local=False):
    print("Loading: ", s_filePath)
    matFile = loadmat(s_filePath)

    f_name, f_ext = os.path.splitext(s_filePath)
    s_FileName = f_name.split("\\")[-1]
    startTime = convertTime(s_FileName)

    db = connectMongoDB(local)
    errorMessages = []
    for i, channel in enumerate(matFile.keys()):
        if ls_ChannelWL and not channel in ls_ChannelWL or ls_ChannelBL and channel in ls_ChannelBL:
            continue
        if channel in ["__header__", "__version__", "__globals__"]:
            continue
        for ii, message in enumerate(matFile[channel][0][0].dtype.fields.keys()):
            if ls_MessageWL and not message in ls_MessageWL:
                continue
            # TODO: Fix Failing messages:
            if message == "FMS" or "DataFrame" in message: 
                continue
            d = {}
            if message == "SoundAI" or message == "AI000" or message == "AI001": #SoundAI is a single Sensor and does not have multiple signals
                signal = message
                if isSignalNameInDB(db, message, signal):
                    if isSignalUploaded(db, channel, message, signal, matFile, startTime ):
                        #print(frameinfo.filename, frameinfo.lineno)
                        #print("Skipping: ", channel, message, signal)
                        continue
                d = [dict([("_id", (x[0]+startTime)*100000), (signal, x[1])]) for x in matFile[channel][0][0][message]]
            else:
                try:
                    for iii, signal in enumerate(matFile[channel][0][0][message].dtype.fields.keys()):
                        # TODO: Fix Failing signals:
                        if "DataFrame" in signal or "ErrorFrame" in signal:
                            continue
                        if ls_SignalWL and not signal in ls_SignalWL:
                            continue
                        if isSignalNameInDB(db, message, signal):
                            if isSignalUploaded(db, channel, message, signal, matFile, startTime ):
                                #print(frameinfo.filename, frameinfo.lineno)
                                #print("Skipping: ", channel, message, signal)
                                continue
                        else: 
                            # get two lists, first containing the names of signals as string [<signal1>, <signal2>, ...] 
                            #   second containing [[timestamps (as ID)], [<signal1_values>], [<signal2_values>], ...]
                            if not d: # extract timestamps once, as they are the same for all signals in a message
                                d = [dict([("_id", (x[0]+startTime)*100000), (signal, x[1])]) for x in matFile[channel][0][0][message][signal][0][0]]
                            else:
                                #print("#5", iii, signal)
                                [d[i].update([(signal, x[1])]) for i, x in enumerate(matFile[channel][0][0][message][signal][0][0])]
                except:
                    print("Exception in signal Loop: ", message, signal)
                
            # combine timestamps from measurement with start time of measurement to create identifier in database
            collist = db.list_collection_names()
            if message in collist:
                #print("The collection exists.")
                col = db[message]
            else: 
                col = db[message]

                try:
                    lock.acquire()
                    col.insert_many( d )
                except Exception as ex:
                    if not message in errorMessages:
                        errorMessages.append(message)
                        print("E2: An exception occurred", channel, message)
                finally:
                    lock.release()     

def getEarliestID(db):
    l_colNames = db.list_collection_names()
    earliestDatabaseID = None
    for colName in l_colNames:
        earliestCollectionID = db[colName].find_one({"$query":{},"$orderby":{"_id":1}})["_id"]
        if not earliestDatabaseID or earliestDatabaseID > earliestCollectionID:
            earliestDatabaseID = earliestCollectionID
    return earliestDatabaseID

def getlatestID(db, channel=None, message=None, startWithID=None, endWithID=None):
    l_colNames = db.list_collection_names()
    earliestDatabaseID = None
    for colName in l_colNames:
        earliestCollectionID = db[colName].find_one({"$query":{},"$orderby":{"_id":-1}})["_id"]
        if not earliestDatabaseID or earliestDatabaseID > earliestCollectionID:
            earliestDatabaseID = earliestCollectionID
    return earliestDatabaseID

def getEarliestMessageID(db, message):
    return db[message].find_one({"$query":{},"$orderby":{"_id":1}})["_id"]
    
def getLatestMessageID(db, message):
    return db[message].find_one({"$query":{},"$orderby":{"_id":-1}})["_id"]

def isSignalNameInDB(db, message, signal):
    ''' Returns true if the name of the signal is a column in the collection with messages name in the database'''
    if isMessageInDB(db, message):
        return signal in db[message].find_one().keys()
    else:
        return False

def isMessageInDB(db, message):
    ''' Returns true if the name of the message is a collection in the database'''
    return db.get_collection(message).find_one() is not None

def areMessageSignalsInDB(db, message, matFile, startTime):
    d_start = {}
    d_end = {}
    for signal in message.dtype.fields.keys():
        earliestSignalTimeDelta =   message[signal][0][0][0][0]
        earliestSignalValue =       message[signal][0][0][0][1]
        latestSignalTimeDelta =     message[signal][0][0][-1][0]
        latestSignalValue =         message[signal][0][0][-1][1]
        t0 = (startTime + earliestSignalTimeDelta)*100000
        t1 = (startTime + latestSignalTimeDelta)*100000
        if not d_start:
            d_start = {"_id": t0, signal: earliestSignalValue}
            d_end = {"_id": t1, signal: latestSignalValue}
        else:
            d_start.update({signal: earliestSignalValue})
            d_end.update({signal: latestSignalValue})

    return d_start, d_end



def isSignalUploaded(db, channel, message, signal, matFile, startTime):
    earliestSignalTimeDelta = matFile[channel][0][0][message][signal][0][0][0][0]
    earliestSignalValue = matFile[channel][0][0][message][signal][0][0][0][1]
    latestSignalTimeDelta = matFile[channel][0][0][message][signal][0][0][-1][0]
    latestSignalValue = matFile[channel][0][0][message][signal][0][0][-1][1]
    t0 = (startTime + earliestSignalTimeDelta)*100000
    t1 = (startTime + latestSignalTimeDelta)*100000
    return db[message].find_one({"_id": t0},{signal: earliestSignalValue}) and \
           db[message].find_one({"_id": t1},{signal: latestSignalValue})

def getMongoMessage(s_MessageName, s_SignalName=None):
    from pymongo import MongoClient, ASCENDING
    import pandas as pd
    
    # connect to MongoDB
    client = MongoClient("127.0.0.1", port=27017)
    db = client.InFusion # use InFusion database
    col = db[s_MessageName] # get the collection
    df = pd.DataFrame(list(col.find().sort([("_id", ASCENDING)]))) # get the data sorted with ascending id
    if s_SignalName:
        df = df[s_SignalName]
    return df


def signalsExistInDB(dbCollection, d_matFileMessageKeys) -> list:
    return [matFileMessageKey for matFileMessageKey in d_matFileMessageKeys if matFileMessageKey not in dbCollection.find_one().keys()]
