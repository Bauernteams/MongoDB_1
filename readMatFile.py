from scipy.io import loadmat #load matlab files in python

def readMatFile(s_filePath):
    matFile = loadmat(s_filePath)

    for i, channel in enumerate(matFile.keys()):
    #print("#1", channel)
        if channel in ["__header__", "__version__", "__globals__"]:
        #print("#2", channel)
            continue
    #print("#3", channel)
    for ii, message in enumerate(matFile[channel][0][0].dtype.fields.keys()):
        #print("#4", ii, message)
        if message == "SoundAI": #SoundAI is a single Sensor and does not have multiple signals
            t, v = [list(x) for x in zip(*matFile[channel][0][0][message])]
            #TODO Add t,v into Database
            continue
        for iii, signal in enumerate(matFile[channel][0][0][message].dtype.fields.keys()):
            #print("#5", iii, signal)
            t, v = [list(x) for x in zip(*matFile[channel][0][0][message][signal][0][0])]
            #TODO Add t,v into Database