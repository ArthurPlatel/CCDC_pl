import pandas as pd
import csv
import os
import glob
from datetime import date
import argparse
from ccd import detect
import time
from ccd.parameters import defaults as dfs
import json

def readCSV(csvFile):
    
    #Load all CSV files
    allFiles = glob.glob(os.path.join(csvFile,'*m.csv'))
    ordered=sorted(allFiles)
    data=pd.read_csv(ordered[0])
    pixels=eval(data['pixels'][0])
    shape=eval(data['shape'][0])
    sample_size=(data['sample_size'])
    bands,rows,cols=shape
    p0,p1,num=pixels
    pixel_count=p1-p0
    with open(str(csvFile)+'/'+str(num)+'_Results.json', 'w') as outfile:
            json.dump([],outfile)
    #add pixel coordinate`s
    for pix in range(pixel_count):
        start=time.time()
        pixelData=[[]for k in range(7)]
        for k in range(len(data)):
            nir=eval(data['nirs'][k])[pix]
            red=eval(data['reds'][k])[pix]
            ndvi=(int((nir-red)/(nir+red)*1000))
            pixelData[0].append(data['date'][k])
            pixelData[1].append(eval(data['blues'][k])[pix])
            pixelData[2].append(eval(data['greens'][k])[pix])
            pixelData[3].append(red)
            pixelData[4].append(nir)
            pixelData[5].append(ndvi)
            pixelData[6].append(1)
        dates, blues, greens, reds, nirs, ndvis, qas = pixelData
        results = detect(dates, blues, greens, reds, nirs, ndvis, qas,dfs['params'])
        end=time.time()-start
        pixel=tuple(((pix+p0)//cols,(pix+p0)%cols))
        ccdResults={str(pixel):results}
        with open(str(csvFile)+'/'+str(num)+'_Results.json', 'r') as outfile:
            dat=json.load(outfile) 
            dat.append(ccdResults)
        with open(str(csvFile)+'/'+str(num)+'_Results.json', "w") as file:
            json.dump(dat, file)
        
        print("processed pixel {} in {}".format(pixel,end))
        
        

        
   
def main():
    parser = argparse.ArgumentParser(description="CCDC", allow_abbrev=False, add_help=False)
    parser.add_argument("-f",action="store", metavar="action", type=str, help="choose function ( i= init, a = addImages)")
    args = parser.parse_args()
    csv_dir=args.f
    allFiles = glob.glob(os.path.join(csv_dir, '*m.csv'))
    csvFile=allFiles[0]
    readCSV(csv_dir)



if __name__ == '__main__':
    main()

   
   