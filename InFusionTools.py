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
        f_name = f_name.replace("mat", "")
        #f_ext = f_ext.replace('mat.', '.')
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
        data = MDF(fullpath)
        Y = str(data.start_time.year)
        M = str(data.start_time.month)
        D = str(data.start_time.day)
        h = str(data.start_time.hour)
        m = str(data.start_time.minute)
        s = str(data.start_time.second)


        _, f_ext = os.path.splitext(fn)
        f_name = "_".join(["Recorder", Y, M, D, h, m, s])
        
        #f_ext = f_ext.replace('mat.', '.')
        new_name = f'{f_name}{f_ext}'
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

if __name__ == "__main__":
    renameFilesInFolderRekorder(r"C:\Users\broll\ZF Friedrichshafen AG\33658 InFusion - Documents\Grunddatenerhebung\04 Data\20210603_extracted\20210609\mat_Export\extracted")
    #pass