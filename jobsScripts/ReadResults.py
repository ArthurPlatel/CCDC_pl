import pandas as pd
import csv
from ccd.parameters import defaults as dfs
import os
import glob
from ccd import detect
import json
import argparse
import time

def main():
    
    ###Argparse settings
    parser = argparse.ArgumentParser(description="CCDC", allow_abbrev=False, add_help=False)
    parser.add_argument("-d",action="store", metavar="directory", type=str, help="Directory of image stack")
    args = parser.parse_args()
    
    #variables
    parent_dir=args.d
    csv_dir= str(parent_dir)+'/pixelValues'
    files = glob.glob(os.path.join(csv_dir, '*m.csv'))
    imageData=glob.glob(os.path.join(csv_dir, 'imageData*.csv'))[0]
    
    #run code on each data CSV file in directory/can change to single file
    #for file in files:
    readResults(files[0], imageData,csv_dir)

def readResults(file, imageData,csv_dir):

    #read csv file and imageData CSV
    df=pd.read_csv(file,header=0)
    df2=pd.read_csv(imageData, header=None)
   
    ####Variables######
    pixel_count=int(df2[1][0])
    shape=eval(df2[2][0])
    dates =eval(df2[4][0])
    params=dfs['params']
    qas=[1]*len(dates)
    pixel1=eval(df['pixel'][0])
    num=(((pixel1[0]*shape[1])+pixel1[1]))//pixel_count
    
    #determine CSV number to write
    with open(str(csv_dir)+'/'+str(num)+'_Results.json', 'w') as outfile:
            json.dump([],outfile)
    
    ####Calculate CCD for each pixel
    print("calulating CCD Results")
    for pix in range(pixel_count):
        start=time.time()
        pixel=eval(df['pixel'][pix])
        data =[[eval(df[band][pix])]for band in df.keys()[2:7]]
        blues,greens,reds,nirs,ndvis=data
        results=detect(dates,blues[0],greens[0],reds[0],nirs[0],ndvis[0],qas,params)
        ##write results to json
        ccdResults={str(pixel):results}
        with open(str(csv_dir)+'/'+str(num)+'_Results.json', 'r') as outfile:
            dat=json.load(outfile) 
            dat.append(ccdResults)
        with open(str(csv_dir)+'/'+str(num)+'_Results.json', "w") as file:
            json.dump(dat, file)
        end=time.time()-start
        print('processed pixel {} in {}'.format(pixel,end))


if __name__ == '__main__':
    main()