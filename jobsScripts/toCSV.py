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




# def addConstruct(images,shape,sample_size,output,proj,geo,pixels):
#     dates=[]
#     for file in images:
#         dates.append(date(int(file[-14:-10]), int(file[-9:-7]), int(file[-6:-4])).toordinal()) 
#     bands,rows,columns=shape
#     p0,p1,n=pixels
#     num=0
#     #create image data csv
#     for image in images:
#         print("processing image {}".format(num))
#         num+=1
#         start=time.time()
#         if sample_size!=3:
#             resampled=gdal.Warp('/vsimem/in_memory_output.tif',file,xRes=sample_size,yRes=sample_size,resampleAlg=gdal.GRA_Average)
#             tile=resampled
#         else:
#             tile=gdal.Open(file)
#         values=flatten=[tile.GetRasterBand(k+1).ReadAsArray().flatten()[p0:p1]for k in range(bands)]
#         write_CSV(values,shape,sample_size,output,pixels)
#         end=time.time()-start
#         print('processed in {}'.format(end))




#### Creates empty CSV files based based on user selected pixel_count

def create_CSV(shape,sample_size,output,pixels):
    
    ######Variables######
    p0,p1,n=pixels
    pixel_count=p1-p0
    fields=['pixel','blue','green','red','nir','ndvi']
    bands,rows,cols=shape
    print("creating CSV {}".format(n))
    #### create empty lists for each pixel to later append
    data=[[[]for k in range(6)]for pix in range(pixel_count)]

    #add pixel coordinates
    for pix in range(pixel_count):
        data[pix][0].append(((pix+p0)//cols,(pix+p0)%cols))
    
    #store empty lists as DataFrame and write to CSV
    df=pd.DataFrame(data)
    df.to_csv(str(output)+'/'+str(n)+'_'+str(sample_size)+'m.csv',header=fields)

####Adds new image pixel values to previous time series###
####and overwrites CSV file with new data################

def write_CSV(values,shape,sample_size,output,pixels):
    ##Variables
    bands,rows,cols=shape
    p0,p1,n=pixels
    pixel_count=p1-p0
    name=str(n)+'_'+str(sample_size)+'m.csv'
    fields=['pixel','blue','green','red','nir','ndvi']
    
    ## Read CSV to overwrite
    csv=glob.glob(os.path.join(output,name))
    data=pd.read_csv(csv[0],header=0,names=fields)

    # put CSV data into DataFramef
    df=pd.DataFrame(data)
    new=[[eval(df[k][pix])for k in fields[1:]]for pix in range(len(df))]


    #insert pixel coordinates
    for pix in range(pixel_count):
        new[pix].insert(0,((pix+p0)//cols,(pix+p0)%cols))
    
    #append new pixel data to band lists
    for pix in range(len(new)):
        for k in range(len(values)):
            new[pix][k+1].append(values[k][pix])
        ndvi = ((((values[3][pix])-(values[2][pix]))/((values[3][pix])+(values[2][pix])))*1000)
        new[pix][5].append(int(ndvi))
    
    #overwrite previous csv with new data
    df2=pd.DataFrame(new)
    df2.to_csv(str(output)+'/'+str(n)+'_'+str(sample_size)+'m.csv',header=fields)

   
#divide total pixels into smaller subets
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
    bands,rows,columns=shape
    p0,p1,n=pixels
    num=0

    ##record dates of images in directory
    dates=[]
    for file in images:
        dates.append(date(int(file[-14:-10]), int(file[-9:-7]), int(file[-6:-4])).toordinal()) 
    #create CSV file with image metadata

    with open(str(output)+'/imageData.csv','w') as dates_file:  
        writer = csv.writer(dates_file)
        writer.writerow([geo,p1-p0,shape,proj,dates])

    ##Open each image in directory and 
    for file in images:
        print("processing image {}".format(num))
        num+=1
        start=time.time()

        ### determine image resolution
        if sample_size!=3:
            resampled=gdal.Warp('/vsimem/in_memory_output.tif',file,xRes=sample_size,yRes=sample_size,resampleAlg=gdal.GRA_Average)
            tile=resampled
        else:
            tile=gdal.Open(file)

        #flatten image array for easier data extraction
        values=[tile.GetRasterBand(k+1).ReadAsArray().flatten()[p0:p1]for k in range(bands)]
        
        #append pixel values to CSV
        write_CSV(values,shape,sample_size,output,pixels)

        end=time.time()-start
        print('processed in {}'.format(end))


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
    imageData= glob.glob(os.path.join(output,'*imageData.csv'))
    allFiles = glob.glob(os.path.join(parent_dir, '*.tif'))
    #sort images chronologically 
    sortedFiles=sorted(allFiles)
    

    #collect all image dates in list
    datesNow=[]
    for file in sortedFiles:
        datesNow.append(date(int(file[-14:-10]), int(file[-9:-7]), int(file[-6:-4])).toordinal()) 
    
    ##collect variables from CSV
    with open(imageData[0]) as csvFile:
        read=pd.read_csv(csvFile, delimiter=',',header=None,names=list(range(5))).dropna(axis='columns',how='all')
        ###Variables From CSV###
        geo=eval(read[0][0])
        sample_size=int(geo[1])
        pixel_count=int(read[1][0])
        shape=eval(read[2][0])
        proj=read[3][0]
        datesBefore=eval(read[4][0])
        #######################
        
        #empty lists to store new image info
        newFiles=[]
        newDates=[]
        numAdded=0
        
        #determine which images are new
        for day in datesNow:
            if datesBefore.count(day)==0 and day > newDates[-1]:
                file=str(date.fromordinal(day))+'*.tif'
                image= glob.glob(os.path.join(parent_dir,file))
                #store new image file name and dates in new lists
                newFiles.append(image[0])
                newDates.append(day)
                print("adding tif: {}".format(image[0]))
                numAdded+=1
            else:
                newDates.append(day)
    
    ##replace imageData with updated date lists
    with open(imageData[0], 'w') as csvFile:
        writer=csv.writer(csvFile)
        writer.writerow([str(geo),int(pixel_count),tuple(shape),str(proj),newDates])
        pixels=pixelsPool(shape,pixel_count)
        p = multiprocessing.Pool(cores)
        p.map(partial(construct,newFiles,shape,sample_size,output,proj,geo),pixels)
        print("Added {} images".format(numAdded))


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