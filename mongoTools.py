from typing import Dict
from pymongo import MongoClient
from scipy.io import loadmat
import os
from InFusionTools import convertTime
import numpy as np
from multiprocessing import Process, Lock
import certifi

def mongoUpload(col, t, v):
    
    # Connect to the MongoDB on localHost
    #TODO: Create MongoDB on ZF-Server
    client = MongoClient("127.0.0.1", port=27017)

    # Check if "InFusion"-Database already exists, else create the database "InFusion"
    dblist = client.list_database_names()
    if "InFusion" in dblist:
        print("The database exists.")
        db = client.InFusion
    else: 
        db = client["InFusion"]

    # debugging
    # Check if "test"-Collection already exists, else create the collection "test"
    #TODO replace collection with meaningfull collection for InFusion data
    collist = db.list_collection_names()
    if col in collist:
        print("The collection exists.")
        col = db[col]
    else: 
        col = db[col]

    

    # Collection name: <message>
    # collection content:
    # _id: timestamp
    # column names: <signal>
    mylist = [
    { "_id": 1, "name": "John", "address": "Highway 37"},
    { "_id": 2, "name": "Peter", "address": "Lowstreet 27"},
    { "_id": 3, "name": "Amy", "address": "Apple st 652"},
    { "_id": 4, "name": "Hannah", "address": "Mountain 21"},
    { "_id": 5, "name": "Michael", "address": "Valley 345"},
    { "_id": 6, "name": "Sandy", "address": "Ocean blvd 2"},
    { "_id": 7, "name": "Betty", "address": "Green Grass 1"},
    { "_id": 8, "name": "Richard", "address": "Sky st 331"},
    { "_id": 9, "name": "Susan", "address": "One way 98"},
    { "_id": 10, "name": "Vicky", "address": "Yellow Garden 2"},
    { "_id": 11, "name": "Ben", "address": "Park Lane 38"},
    { "_id": 12, "name": "William", "address": "Central st 954"},
    { "_id": 13, "name": "Chuck", "address": "Main Road 989"},
    { "_id": 14, "name": "Viola", "address": "Sideway 1633"}
    ]

    x = col.insert_many(mylist)

def mongoUploadFile(s_filePath, ls_SignalWL=None, ls_MessageWL=None, ls_ChannelWL = None):
    print("Loading: ", s_filePath)
    matFile = loadmat(s_filePath)

    f_name, f_ext = os.path.splitext(s_filePath)
    s_FileName = f_name.split("\\")[-1]
    t0 = convertTime(s_FileName)

    db = connectMongoDB()
    errorMessages = []
    for i, channel in enumerate(matFile.keys()):
        if ls_ChannelWL and not channel in ls_ChannelWL:
            continue
        if channel in ["__header__", "__version__", "__globals__"]:
            continue
        for ii, message in enumerate(matFile[channel][0][0].dtype.fields.keys()):
            if ls_MessageWL and not message in ls_MessageWL:
                continue
            # TODO: Fix Failing messages:
            if message == "FMS" or "DataFrame" in message: 
                continue
            if message == "SoundAI": #SoundAI is a single Sensor and does not have multiple signals
                l_timeStamps, l_values = [list(x) for x in zip(*matFile[channel][0][0][message])]
                l_signalNames = ["SoundAI"]
            else:
                l_signalNames = []
                l_values = []
                for iii, signal in enumerate(matFile[channel][0][0][message].dtype.fields.keys()):
                    # TODO: Fix Failing signals:
                    if "DataFrame" in signal:
                        continue
                    # get two lists, first containing the names of signals as string [<signal1>, <signal2>, ...] 
                    #   second containing [[timestamps (as ID)], [<signal1_values>], [<signal2_values>], ...]
                    l_signalNames.append(signal)
                    if iii == 0: # extract timestamps once, as they are the same for all signals in a message
                        l_timeStamps, temp = [list(x) for x in zip(*matFile[channel][0][0][message][signal][0][0])]
                        l_values.append(temp)
                    else:
                        print("#5", iii, signal)
                        _, temp = [list(x) for x in zip(*matFile[channel][0][0][message][signal][0][0])]
                        l_values.append(temp)
                
            # combine timestamps from measurement with start time of measurement to create identifier in database
            l_timeStamps = [np.round((t0 + td) * 100000) for td in l_timeStamps]
            collist = db.list_collection_names()
            if message in collist:
                #print("The collection exists.")
                col = db[message]
            else: 
                col = db[message]
            
            for iv in range(len(l_timeStamps)):
                upload = {"_id": l_timeStamps[iv]}
                if message == "SoundAI":
                    sig = "SoundAI"
                    upload.update({sig: l_values[iv]})
                else:
                    for v, sig in enumerate(l_signalNames):
                        #print("#6", sig)
                        try:
                            upload.update({sig: l_values[v][iv]})
                        except:
                            if not message in errorMessages:
                                errorMessages.append(message)
                                print("E1: An exception occurred", channel, message,  sig, v, iv)
                try:
                    col.insert_one( upload )
                except:
                    if not message in errorMessages:
                        errorMessages.append(message)
                        print("E2: An exception occurred", channel, message,  sig, iv)
                    

def connectMongoDB():    
    # connect to MongoDB
    ca = certifi.where()
    client = MongoClient("mongodb+srv://Bauernteams:4FCQK4LPBXSY4Ss@cluster0.dazrx.mongodb.net/myFirstDatabase?retryWrites=true&w=majority", tlsCAFile=ca)
    db = client.InFusion # use InFusion database
    return db

def mongoUploadFileMulti(lock, s_filePath, ls_SignalWL=None, ls_MessageWL=None, ls_ChannelWL = None, ls_ChannelBL=None):
    print("Loading: ", s_filePath)
    matFile = loadmat(s_filePath)

    f_name, f_ext = os.path.splitext(s_filePath)
    s_FileName = f_name.split("\\")[-1]
    t0 = convertTime(s_FileName)

    db = connectMongoDB()
    errorMessages = []
    for i, channel in enumerate(matFile.keys()):
        if ls_ChannelWL and not channel in ls_ChannelWL or channel in ls_ChannelBL:
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
                    # get two lists, first containing the names of signals as string [<signal1>, <signal2>, ...] 
                    #   second containing [[timestamps (as ID)], [<signal1_values>], [<signal2_values>], ...]
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
                    if not message in errorMessages:
                        errorMessages.append(message)
                        print("E2: An exception occurred", channel, message)
                finally:
                    lock.release()

def mongoUploadFileMulti2(s_filePath, ls_SignalWL=None, ls_MessageWL=None, ls_ChannelWL = None, ls_ChannelBL=None):
    print("Loading: ", s_filePath)
    matFile = loadmat(s_filePath)

    f_name, f_ext = os.path.splitext(s_filePath)
    s_FileName = f_name.split("\\")[-1]
    t0 = convertTime(s_FileName)

    errorMessages = []
    for i, channel in enumerate(matFile.keys()):
        if ls_ChannelWL and not channel in ls_ChannelWL or channel in ls_ChannelBL:
            continue
        if channel in ["__header__", "__version__", "__globals__"]:
            continue      
        
        my_lock = Lock()
        for ii, message in enumerate(matFile[channel][0][0].dtype.fields.keys()):
            p_files = Process(target=uploadMesage, args=(message, matFile, channel, my_lock, ls_MessageWL, ls_SignalWL, t0))
            p_files.start()
        p_files.join()


def uploadMesage(message, matFile, channel, lock, ls_MessageWL, ls_SignalWL, t0):
    db = connectMongoDB()
    if ls_MessageWL and not message in ls_MessageWL:
        return
    # TODO: Fix Failing messages:
    if message == "FMS" or "DataFrame" in message: 
        return
    if message == "SoundAI": #SoundAI is a single Sensor and does not have multiple signals
        l_timeStamps, l_values = [list(x) for x in zip(*matFile[channel][0][0][message])]
        l_signalNames = ["SoundAI"]
    else:
        l_signalNames = []
        l_values = []
        for iii, signal in enumerate(matFile[channel][0][0][message].dtype.fields.keys()):
            # TODO: Fix Failing signals:
            if "DataFrame" in signal:
                continue
            if ls_SignalWL and not signal in ls_SignalWL:
                continue
            # get two lists, first containing the names of signals as string [<signal1>, <signal2>, ...] 
            #   second containing [[timestamps (as ID)], [<signal1_values>], [<signal2_values>], ...]
            l_signalNames.append(signal)
            if iii == 0: # extract timestamps once, as they are the same for all signals in a message
                l_timeStamps, temp = [list(x) for x in zip(*matFile[channel][0][0][message][signal][0][0])]
                l_values.append(temp)
            else:
                #print("#5", iii, signal)
                temp = [x[1] for x in matFile[channel][0][0][message][signal][0][0]]
                l_values.append(temp)
        
    # combine timestamps from measurement with start time of measurement to create identifier in database
    l_timeStamps = [np.round((t0 + td) * 100000) for td in l_timeStamps]
    collist = db.list_collection_names()
    if message in collist:
        #print("The collection exists.")
        col = db[message]
    else: 
        col = db[message]
    
    for iv in range(len(l_timeStamps)):
        upload = {"_id": l_timeStamps[iv]}
        if message == "SoundAI":
            sig = "SoundAI"
            upload.update({sig: l_values[iv]})
        else:
            for v, sig in enumerate(l_signalNames):
                #print("#6", sig)
                try:
                    upload.update({sig: l_values[v][iv]})
                except:
                    pass
        try:
            lock.acquire()
            col.insert_one( upload )
        except Exception as ex:
            pass
        finally:
            lock.release()