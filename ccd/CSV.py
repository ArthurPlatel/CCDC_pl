from osgeo.gdalconst import GA_ReadOnly
from ArgParse import ArgumentParser
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


#######Functions##########
#Get Image Info
def getInfo(allFiles,sample_size):
    #open a single image file to extract values
    file=gdal.Open(allFiles[0])
    
    #resample image to requested resoultion
    if sample_size!=3:
        resampled=gdal.Warp('/vsimem/in_memory_output.tif',file,xRes=sample_size,yRes=sample_size,resampleAlg=gdal.GRA_Average)
        image=resampled
    else:
        image=file

    #output variables
    shape=np.shape(image.ReadAsArray())
    proj=image.GetProjection()
    geo=image.GetGeoTransform()
    return shape,proj,geo

def toCSV(image):
    print(image)


def main():
    ############ Command Line Variables ################
    ## Set Parser flags
    parser = ArgumentParser(description="CCDC", allow_abbrev=False, add_help=False)
    parser.add_argument("-d",action="store", metavar="directory", type=str, help="Directory of image stack")
    parser.add_argument("--r", action="store", metavar="value", type=int, help="Output resolution to resample image stack", default=30)
    parser.add_argument("--p", action="store", metavar="value", type=int, help="Number of pixels to process per CSV file", default=800)
    parser.add_argument("--c", action="store", metavar="value", type=int, help="Number of cores to use in multiprocessing", default=4)
    parser.add_argument("-h", "--help", action="help", help="Display this message")
    args = parser.parse_args()
    ## Get Variables
    parent_dir=args.d
    sample_size=args.r
    pixel_count=args.p
    cores=args.c
    ###################
    parent_dir= '/Users/arthur.platel/Desktop/PF-SR'
    images = glob.glob(os.path.join(parent_dir, '*.tif'))
    sortedFiles=sorted(images)
    output=str(parent_dir)+'/pixelValues'
    shape,proj,geo=getInfo(sortedFiles,sample_size)
    bands,rows,cols=shape
    if not os.path.isdir(output):
        os.mkdir(output)
    print(len(sortedFiles))
    file=images[0]
    if sample_size!=3:
        resampled=gdal.Warp('/vsimem/in_memory_output.tif',file,xRes=sample_size,yRes=sample_size,resampleAlg=gdal.GRA_Average)
        image=resampled 
    else:
        image=gdal.Open(file)
    print(bands)
    for pix in range(rows*cols):
        
    #flat=[image.GetRasterBand(k+1).ReadAsArray().flatten()[p0:p1]for k in range(bands)]
    #print(len(flat))

    
    


if __name__ == '__main__':
    main()