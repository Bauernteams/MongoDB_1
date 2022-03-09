import os

# String containing the path to the folder with the files to be renamed
sFolder = r"C:\Users\broll\ZF Friedrichshafen AG\33658 InFusion - Documents\Grunddatenerhebung\07 Winterversuche\Data\Messungen\extracted"

# Change the current directory to specified folder:
os.chdir(sFolder)

# Get a list of all the files in specified folder
lFileNames = os.listdir()


# Run through list of files, rename and save each one
# HERE: replace "-" with "_" in the files' name
for fn in lFileNames:
    f_name, f_ext = os.path.splitext(fn)
    f_name = f_name.replace("mat", "")
    new_name = f'{f_name}{f_ext}'
    os.rename(fn, new_name)