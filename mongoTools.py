from typing import Dict
from pymongo import MongoClient
from scipy.io import loadmat
import os
from InFusionTools import convertTime
import numpy as np
from multiprocessing import Process, Lock
import certifi

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
    t0 = convertTime(s_FileName)

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
                d = [dict([("_id", (x[0]+t0)*100000), (signal, x[1])]) for x in matFile[channel][0][0][message]]
            else:
                try:
                    for iii, signal in enumerate(matFile[channel][0][0][message].dtype.fields.keys()):
                        # TODO: Fix Failing signals:
                        if "DataFrame" in signal or "ErrorFrame" in signal:
                            continue
                        if ls_SignalWL and not signal in ls_SignalWL:
                            continue
                        # get two lists, first containing the names of signals as string [<signal1>, <signal2>, ...] 
                        #   second containing [[timestamps (as ID)], [<signal1_values>], [<signal2_values>], ...]
                        if not d: # extract timestamps once, as they are the same for all signals in a message
                            d = [dict([("_id", (x[0]+t0)*100000), (signal, x[1])]) for x in matFile[channel][0][0][message][signal][0][0]]
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

def uploadMesage(message, matFile, channel, lock, ls_MessageWL, ls_SignalWL, t0, local=False):
    db = connectMongoDB(local)
    if ls_MessageWL and not message in ls_MessageWL:
        return
    # TODO: Fix Failing messages:
    if message == "FMS" or "DataFrame" in message: 
        return
    d = {}
    if message == "SoundAI": #SoundAI is a single Sensor and does not have multiple signals
        signal = "SoundAI"
        d = [dict([("_id", (x[0]+t0)*100000), (signal, x[1])]) for x in matFile[channel][0][0][message]]
    else:
        for iii, signal in enumerate(matFile[channel][0][0][message].dtype.fields.keys()):
            # TODO: Fix Failing signals:
            if "DataFrame" in signal:
                continue
            if ls_SignalWL and not signal in ls_SignalWL:
                continue
            if not d: # extract timestamps once, as they are the same for all signals in a message
                d = [dict([("_id", (x[0]+t0)*100000), (signal, x[1])]) for x in matFile[channel][0][0][message][signal][0][0]]
            else:
                #print("#5", iii, signal)
                [d[i].update([(signal, x[1])]) for i, x in enumerate(matFile[channel][0][0][message][signal][0][0])]
        
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
                pass
            finally:
                lock.release()