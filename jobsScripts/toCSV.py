from osgeo.gdalconst import GA_ReadOnly
from pandas.core.frame import DataFrame
import argparse
import glob
import os
from osgeo import gdal
import numpy as np
import time
from datetime import date
import csv
import multiprocessing
from functools import partial
import pandas as pd






#### Creates empty CSV files based based on user selected pixel_count

def create_CSV(shape,sample_size,output,pixels):
    
    ######Variables######
    p0,p1,n=pixels
    print("creating CSV {}".format(n))
    #### create empty csv files and write header
    fields=['date','shape','sample_size','pixels','blues','greens','reds','nirs']
    with open(str(output)+'/'+str(n)+'_'+str(sample_size)+'m.csv','w') as f:
        writer=csv.writer(f)
        writer.writerow(fields)
        

####Adds new image pixel values to previous time series###
####and overwrites CSV file with new data################

#####function that takes image pixel values and prints them in CSVs
def toCSV(imageStack,shape,num,sample_size,output,bands,pixels):
        p0,p1,n=pixels
        for file in imageStack:
            print("processing image {}".format(num))
            num+=1
            start=time.time()

            #get image date
            day=date(int(file[-14:-10]), int(file[-9:-7]), int(file[-6:-4])).toordinal()

            ### determine image resolution
            if sample_size!=3:
                resampled=gdal.Warp('/vsimem/in_memory_output.tif',file,xRes=sample_size,yRes=sample_size,resampleAlg=gdal.GRA_Average)
                tile=resampled
            else:
                tile=gdal.Open(file)


            #flatten image array for easier data extraction
            values=[(tile.GetRasterBand(k+1).ReadAsArray().flatten()).tolist()[p0:p1]for k in range(bands)]

            #append pixel values to CSV
            write_CSV(values,day,shape,sample_size,output,pixels)
            end=time.time()-start
            print('processed in {}'.format(end))



### writes a list of image values as CSV row
def write_CSV(values,date,shape,sample_size,output,pixels):
    ##Variables
    p0,p1,n=pixels
    name=output+'/'+str(n)+'_'+str(sample_size)+'m.csv'
    with open(name,mode='a') as f:
        writer=csv.writer(f)
        writer.writerow([date,shape,sample_size,pixels,values[0],values[1],values[2],values[3]])
   
#divide total pixels into smaller subsets
def pixelsPool(shape,pixel_count):
    bands,rows,cols=shape
    tot_pix=rows*cols
    tuples=[]
    jobs=tot_pix//pixel_count
    remain=tot_pix%pixel_count
    for k in range(jobs):
        tuples.append((pixel_count*k,pixel_count*(k+1),k))
    if remain > 0:
        tuples.append(((pixel_count*(jobs)),(pixel_count*(jobs)+remain),(jobs+1)))
    return tuples     

#load image parameters
def getInfo(allFiles,sample_size):
    #open a single image file to extract values
    file=allFiles[0]
    #resample image to requested resoultion
    if sample_size!=3:
        resampled=gdal.Warp('/vsimem/in_memory_output.tif',file,xRes=sample_size,yRes=sample_size,resampleAlg=gdal.GRA_Average)
        image=resampled
    else:
        image=gdal.Open(file)
    #output variables
    shape=np.shape(image.ReadAsArray())
    proj=image.GetProjection()
    geo=image.GetGeoTransform()
    return shape,proj,geo

    ##### Main function to collect image data and write/append to CSV
def construct(images,shape,sample_size,output,proj,geo,pixels):
    
    ###variables
    bands,rows,cols=shape
    p0,p1,n=pixels
    num=0

    ##record dates of images in directory
    dates=[]
    for file in images:
        dates.append(date(int(file[-14:-10]), int(file[-9:-7]), int(file[-6:-4])).toordinal()) 
    #create CSV file with image metadata

    with open(str(output)+'/imageData.csv','w') as dates_file:  
        writer = csv.writer(dates_file)
        writer.writerow([geo,p1-p0,shape,proj])

    ##Open each image in directory and 
    toCSV(images,shape,num,sample_size,output,bands,pixels)
    
#####################################
#####################################

   ####Add Image Function#######
   #############################
   ##Search through directory for new 
   ##images adds their values to CSV files

def addImages(parent_dir,cores):
    #Location of csv files
    output=str(parent_dir)+'/pixelValues'
    
    #Find imageData CSV and all fusion tiles
    allCSV = glob.glob(os.path.join(output,'*m.csv'))
    allFiles = glob.glob(os.path.join(parent_dir, '*.tif'))
    #sort images chronologically 
    sortedFiles=sorted(allFiles)
    #################
    allDates=[0]
    addDates=[]
    #collect all image dates in list
    for file in sortedFiles:
        allDates.append(date(int(file[-14:-10]), int(file[-9:-7]), int(file[-6:-4])).toordinal()) 
    ##collect variables from CSV
    data=pd.read_csv(allCSV[0])
    sample_size=int(data['sample_size'][0])
    oldDates=data['date'].tolist()
    shape=eval(data['shape'][0])
    pixel=eval(data['pixels'][0])
    bands,rows,cols=shape
    pixel_count=pixel[1]-pixel[0]

    #determine how many CSVs to write to
    pixels=pixelsPool(shape,pixel_count)
    
    #determine which dates are new 
    for day in allDates:
        if oldDates.count(day)==0 and day>oldDates[-1] :
            dat=date.fromordinal(day)
            file = glob.glob(os.path.join(parent_dir, str(dat)+'*.tif'))
            addDates.append(file[0])
    num=0
   
    #add dates images not already processed to csv
    p = multiprocessing.Pool(cores)
    print(addDates)
    p.map(partial(toCSV,addDates,shape,num,sample_size,output,bands),pixels)
    
            

        
    #######################``
    
    #empty lists to store new image info
    newFiles=[]
    newDates=[]
    numAdded=0
    
   ####Init Function#######
   ########################
   ## creates pixesl time series data
   ## and stores values in CSV files 

def init(parent_dir, pixel_count,cores,sample_size):
    
    #load and sort files from directory
    allFiles = glob.glob(os.path.join(parent_dir, '*.tif'))
    sortedFiles=sorted(allFiles)

    #create csv output folder
    output=str(parent_dir)+'/pixelValues'
    if not os.path.isdir(output):
        os.mkdir(output)

    #get fusion tile parameters
    shape,proj,geo=getInfo(sortedFiles,sample_size)
    
    #divide total pixels into smaller subets 
    pixels=pixelsPool(shape,pixel_count)
    
    # initiate multiprocessing
    p = multiprocessing.Pool(cores)

    #create blank csv files to be written later
    p.map(partial(create_CSV,shape,sample_size,output),pixels)

    # write and append all pixel data to CSV files
    p.map(partial(construct,sortedFiles,shape,sample_size,output,proj,geo),pixels)
    

def main():
    ############ Command Line Variables ################
    ## Set Parser flags
    parser = argparse.ArgumentParser(description="CCDC", allow_abbrev=False, add_help=False)
    parser.add_argument("-f",action="store", metavar="action", type=str, help="choose function ( i= init, a = addImages)")
    parser.add_argument("-d",action="store", metavar="directory", type=str, help="Directory of image stack")
    parser.add_argument("--r", action="store", metavar="value", type=int, help="Output resolution to resample image stack", default=30)
    parser.add_argument("--p", action="store", metavar="value", type=int, help="Number of pixels to process per CSV file", default=400*400)
    parser.add_argument("--c", action="store", metavar="value", type=int, help="Number of cores to use in multiprocessing", default=4)
    parser.add_argument("-h", "--help", action="help", help="Display this message")
    args = parser.parse_args()
    ## Get Variables
    parent_dir=args.d
    sample_size=args.r
    pixel_count=args.p
    cores=args.c
    if args.f.upper()=='I':
        init(parent_dir,pixel_count,cores,sample_size)
    if args.f.upper()=='A':
        addImages(parent_dir,cores)



if __name__ == '__main__':
    main()