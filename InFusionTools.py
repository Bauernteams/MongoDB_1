TIMESTAMPFACTOR = 100000.0

def convertTime(s_DateTime):
    import time
    import datetime

    string = s_DateTime
    if string.split('_')[0] == "Recorder":
        import os
        string = string.replace("c", "k")
        
    ret = time.mktime(datetime.datetime.strptime(string,
                                                "Rekorder_%Y_%m_%d_%H_%M_%S").timetuple())
    #print(ret)
    return ret

def getDateFromInFusionID(id):
    import datetime
    timestamp = id / TIMESTAMPFACTOR
    return datetime.datetime.fromtimestamp(timestamp).isoformat()

def renameFilesInFolder(sFolder):
    import os

    # String containing the path to the folder with the files to be renamed
    #sFolder = r"C:\Users\broll\ZF Friedrichshafen AG\33658 InFusion - Documents\Grunddatenerhebung\04 Data\20210603\20210609"

    # Change the current directory to specified folder:
    os.chdir(sFolder)

    # Get a list of all the files in specified folder
    lFileNames = os.listdir()


    # Run through list of files, rename and save each one
    # HERE: replace "-" with "_" in the files' name
    for fn in lFileNames:
        f_name, f_ext = os.path.splitext(fn)
        #f_name = f_name.replace("mat", "")
        f_ext = f_ext.replace('mat.', '.')
        new_name = f'{f_name}{f_ext}'
        os.rename(fn, new_name)

def renameFilesInFolderDate(sFolder):
    import os
    from asammdf import MDF

    # String containing the path to the folder with the files to be renamed
    #sFolder = r"C:\Users\broll\ZF Friedrichshafen AG\33658 InFusion - Documents\Grunddatenerhebung\04 Data\20210603\20210609"

    # Change the current directory to specified folder:
    os.chdir(sFolder)

    # Get a list of all the files in specified folder
    lFileNames = os.listdir()


    # Run through list of files, rename and save each one
    # HERE: replace "-" with "_" in the files' name
    for fn in lFileNames:
        fullpath = os.path.join(sFolder, fn)
        with MDF(fullpath) as data:
            Y = str(data.start_time.year)
            M = str(data.start_time.month).zfill(2)
            D = str(data.start_time.day).zfill(2)
            h = str(data.start_time.hour+1).zfill(2)
            m = str(data.start_time.minute).zfill(2)
            s = str(data.start_time.second).zfill(2)


        _, f_ext = os.path.splitext(fn)
        f_name = "_".join(["Recorder", Y, M, D, h, m, s])
        
        #f_ext = f_ext.replace('mat.', '.')
        new_name = f'{f_name}{f_ext}'
        #print(new_name)
        os.rename(fn, new_name)


def renameFilesInFolderRekorder(sFolder):
    import os

    # String containing the path to the folder with the files to be renamed
    #sFolder = r"C:\Users\broll\ZF Friedrichshafen AG\33658 InFusion - Documents\Grunddatenerhebung\04 Data\20210603\20210609"

    # Change the current directory to specified folder:
    os.chdir(sFolder)

    # Get a list of all the files in specified folder
    lFileNames = os.listdir()


    # Run through list of files, rename and save each one
    # HERE: replace "-" with "_" in the files' name
    for fn in lFileNames:
        f_name, f_ext = os.path.splitext(fn)
        f_name = f_name.replace("c", "k")
        #f_ext = f_ext.replace('mat.', '.')
        new_name = f'{f_name}{f_ext}'
        os.rename(fn, new_name)

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

def checkIfSignalWasUploaded(db, l_timestamps, s_signalName, s_messageName):
    pass

def signalsExistInDB(dbCollection, d_matFileMessageKeys) -> list:
    return [matFileMessageKey for matFileMessageKey in d_matFileMessageKeys if matFileMessageKey not in dbCollection.find_one().keys()]


if __name__ == "__main__":
    renameFilesInFolderDate(r"C:\Users\broll\ZF Friedrichshafen AG\33658 InFusion - Documents\Grunddatenerhebung\04 Data\20201124\01_CAN_LOG")
    #pass