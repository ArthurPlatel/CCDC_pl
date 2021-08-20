import pandas as pd
import time
from osgeo import gdal
import numpy as np
import glob
import os
import multiprocessing
from datetime import date

# parent_dir='/Users/arthur.platel/Desktop/Fusion_Images/Imperial_Subset'
# sample_size=3


def loadImages(parent_dir,sample_size,nth=1):
    allFiles = glob.glob(os.path.join(parent_dir, '*.tif'))
    output=parent_dir+str('/CCD_Output'+str(sample_size))

    #Function to only use every nth image instead of the whole stack
    files=[]
    for k in range(len(allFiles)):
                if k%nth==0:
                    files.append(allFiles[k])
    image0=gdal.Open(files[0])

    #Sort files chronologically assuming file name contains image date
    sortedFiles=sorted(files)

    #resample image to get shape
    if sample_size!=3:
        resampled=gdal.Warp('/vsimem/in_memory_output.tif',image0,xRes=sample_size,yRes=sample_size,resampleAlg=gdal.GRA_Average)
    else:
        resampled=image0
    #Global get info for output files
    geo=resampled.GetGeoTransform()
    proj=resampled.GetProjection()

    #global shape of image
    shape=np.shape(resampled.ReadAsArray())
    print(shape)

    #for testing, otherwise y=shape[2]
    rowLen=shape[2]
    y_size=rowLen

    ras_data=geo,proj,output,shape,y_size,sample_size

    return sortedFiles, ras_data



def inputDataPandas(r0,r1,inputData):
    images,ras_data = inputData
    #save row numbers
    rows=[]
    numRows=r1-r0
    geo,proj,output,shape,y_size,sample_size=ras_data
    for k in range(r0,r1):
        rows.append(k)
   
    #create empty array to store time series data for entire image stack
    time_series=[[[[]for r in range(8)]for y in range(y_size)]for x in range(numRows)]
    pd.DataFrame={'RMSE blue':[gdf.iloc[k][2]for k in range(len(gdf))]}
    # Open every Fusion tif, resample to desired size and extract pixel values for each band into "time_series" array
    #returns array with all pixel values from time series
    print("collecting pixel data from {} images for rows {} to {} using {}".format(len(images),r0,r1-1,str(multiprocessing.current_process())[-42:-30]))
    imageTime=time.time()
    for fusion_tif in images:
        day = date(int(fusion_tif[-14:-10]), int(fusion_tif[-9:-7]), int(fusion_tif[-6:-4]))
        image0=gdal.Open(fusion_tif)
        gordinal = day.toordinal()
        if sample_size!=3:
            image=gdal.Warp('/vsimem/in_memory_output',image0,xRes=sample_size,yRes=sample_size,resampleAlg=gdal.GRA_Average)
        else:
            image=image0
        blue = image.GetRasterBand(1).ReadAsArray()
        green = image.GetRasterBand(2).ReadAsArray()
        red = image.GetRasterBand(3).ReadAsArray()
        nir = image.GetRasterBand(4).ReadAsArray()
        for l in range(numRows):
            x=rows[l]
            for y in range(y_size):
                time_series[l][y][0].append(gordinal)
                time_series[l][y][1].append(blue[x,y]) 
                time_series[l][y][2].append(green[x,y])  
                time_series[l][y][3].append(red[x,y])  
                time_series[l][y][4].append(nir[x,y])    
                time_series[l][y][5].append((((nir[x,y]-red[x,y])/(nir[x,y]+red[x,y])*1000)))
                time_series[l][y][6].append((((green[x,y]-nir[x,y])/(green[x,y]+nir[x,y])*1000)))
                time_series[l][y][7].append(1)
    print("{} completed ingesting images in {}".format(str(multiprocessing.current_process())[-42:-30],time.time()-imageTime))
    return time_series,rows

def main():

    bands=['blue','green','red','nir','ndvi']
    timeSeries=pd.DataFrame({str(band):[10,11] for band in bands})
    # timeSeries.append({str(band[0]):[10,11]},ignore_index=True)#for band in bands)
    # timeSeries.append({str(band[1]):[10,11]},ignore_index=True)
    #print(timeSeries)
    #for k in range()
    for band in bands:
        for k in range(5):
            timeSeries = timeSeries.append(pd.Series({str(band): int(k)}),ignore_index=True)
    print(timeSeries)
    
    
    # inputData=loadImages(parent_dir,3)
    # inputDataPandas(0,1, inputData)




if __name__ == '__main__':
    main()

