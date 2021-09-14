import pandas as pd
import csv
from ccd.parameters import defaults as dfs
import os
import glob
from ccd import detect

def main():
    parent_dir='/Users/arthur.platel/Desktop/PF-SR/'
    csv_dir= parent_dir+'/pixelValues'
    files = glob.glob(os.path.join(csv_dir, '*m.csv'))
    imageData=glob.glob(os.path.join(csv_dir, 'imageData*.csv'))[0]
    #for file in files:
    readResults(files[0], imageData)

def readResults(file, imageData):
    df=pd.read_csv(file,header=0)
    df2=pd.read_csv(imageData, header=None)
    ####Variables######
    pixel_count=int(df2[1][0])
    shape=eval(df2[2][0])
    dates =eval(df2[4][0])
    params=dfs['params']
    qas=[1]*len(dates)
    print(df.keys()[2:7])
    for pix in range(5):
        data =[[eval(df[band][pix])]for band in df.keys()[2:7]]
        blues,greens,reds,nirs,ndvis=data
        results=detect(dates,blues[0],greens[0],reds[0],nirs[0],ndvis[0],qas,params)

   
def getImageData(csv_dir):
    allFiles = glob.glob(os.path.join(csv_dir, '*.mcsv'))

if __name__ == '__main__':
    main()