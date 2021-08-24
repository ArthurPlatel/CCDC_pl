from osgeo import gdal
import glob
import numpy as np
import os
from datetime import date
from datetime import time
import time
from ccd import detect
from parameters import defaults as dfs
import multiprocessing
from functools import partial
import csv
import pandas as pd
pix=0

 #################functions##############

def loadImages(parent_dir,sample_size,nth=1):
    allFiles = glob.glob(os.path.join(parent_dir, '*.tif'))
    output=parent_dir+str('/CCD_Output_'+str(sample_size)+'m')

    #Function to only use every nth image instead of the whole stack
    files=[]
    for k in range(len(allFiles)):
                if k%dfs['nth']==0:
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



def save_raster(arrays,name,ras_data,format='GTiff', dtype = gdal.GDT_Float32):
    #variables
    geo,proj,output,shape,y_size,sample_size=ras_data
    #output variables
    band,rows,cols=shape
    band_count=len(arrays)
    tot_path=str(output)+"/"+str(name)+".tif"
    # Initialize driver & create file
    driver = gdal.GetDriverByName(format)
    Image_out = driver.Create(tot_path,cols,rows,band_count,dtype)
    Image_out.SetGeoTransform(geo)
    Image_out.SetProjection(proj)
    for k in range(band_count):
        Image_out.GetRasterBand(k+1).WriteArray(arrays[k])
    print("Rasters Saved")
    Image_out= None

#resample image to desired pixel size
def resample(images,ras_data):
    geo,proj,output,shape,y_size,sample_size=ras_data
    image=images[0]
    image0=gdal.Open(image)
    day0 = date(int(image[-14:-10]), int(image[-9:-7]), int(image[-6:-4]))
    if sample_size!=3:
        image0=gdal.Warp(output+"/"+str(day0)+'_'+str(sample_size)+'m.tif',image0,xRes=sample_size,yRes=sample_size,resampleAlg=gdal.GRA_Average)
    imageL=images[len(images)-1]
    imageLast=gdal.Open(imageL)
    dayLast = date(int(imageL[-14:-10]), int(imageL[-9:-7]), int(imageL[-6:-4]))
    if sample_size!=3:
        imageLast=gdal.Warp(output+"/"+str(dayLast)+'_'+str(sample_size)+'m.tif',imageLast,xRes=sample_size,yRes=sample_size,resampleAlg=gdal.GRA_Average)
 
 
def resampleImage(images,day,ras_data):
    geo,proj,output,shape,y_size,sample_size=ras_data
    for image in images:
        print(image)
        print(day)
        days = date(int(image[-14:-10]), int(image[-9:-7]), int(image[-6:-4])).toordinal()
        if int(day)==int(days):
            print(day)
            image0=gdal.Open(image)
            if sample_size!=3:
                image0=gdal.Warp(output+"/"+str(date.fromordinal(day))+'_'+str(sample_size)+'m.tif',image0,xRes=sample_size,yRes=sample_size,resampleAlg=gdal.GRA_Average)
 
 ###ingest fusion image stack data#####
def inputData(r0,r1,images,ras_data):
    
    #save row numbers
    rows=[]
    numRows=r1-r0
    geo,proj,output,shape,y_size,sample_size=ras_data
    for k in range(r0,r1):
        rows.append(k)
   
    #create empty array to store time series data for entire image stack
    time_series=[[[[]for r in range(8)]for y in range(y_size)]for x in range(numRows)]
    
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
                time_series[l][y][5].append(((nir[x,y]-red[x,y])/(nir[x,y]+red[x,y]))*1000)
                time_series[l][y][6].append(((green[x,y]-nir[x,y])/(green[x,y]+nir[x,y]))*1000)
                time_series[l][y][7].append(1)
    print("{} completed ingesting images in {}".format(str(multiprocessing.current_process())[-42:-30],time.time()-imageTime))
    return time_series,rows



    
#function to save arrays for every rmse/coefficient from ccd output
#returns 2-d array with pixel values
def changeArray(result_map,day1,day2,ras_data):
    geo,proj,output,shape,y_size,sample_size=ras_data
    emptyArray=np.zeros((shape[1],y_size))
    for r in range(len(result_map)):
        for x in range(len(result_map[r][0])):
            row=result_map[r][1][x]
            for y in range(len(result_map[r][0][x])):
                for seq in result_map[r][0][x][y]:
                    if day1<=seq["end_day"]<day2:
                        emptyArray[row,y]=seq["break_day"]
    save_raster([emptyArray],"Change_"+str(date.fromordinal(day1))+'_To_'+str(date.fromordinal(day2)),ras_data)
    
def toArray(result_map,band,dtp,day,ras_data,k=None):
    geo,proj,output,shape,y_size,sample_size=ras_data
    emptyArray=np.zeros((shape[1],y_size))
    for r in range(len(result_map)):
        for x in range(len(result_map[r][0])):
            row=result_map[r][1][x]
            for y in range(len(result_map[r][0][x])):
                seqn=0
                for seq in (result_map[r][0][x][y]):
                    if seq["start_day"]<=day<=seq["end_day"]:
                        seqn+=1
                        if k is None:
                            emptyArray[row,y]=seq[band][dtp]
                        else:
                            emptyArray[row,y]=seq[band][dtp][k]
    return emptyArray

        
#CCD function for an individual pixel
def CCD(pixel_data,pixel_coordinates):
    now=time.time()
    data=np.array(pixel_data)
    dates,blues,greens,reds,nirs,ndvis,ndwis,qas=data
    result=detect(dates, greens,blues, reds, nirs, ndvis, ndwis, qas, dfs['params'])
    final_time=time.time()-now
    print("processing pixel {}".format(pixel_coordinates))
    return result["change_models"]

#CCD function for set amout of rows, input as tuple 
# i.e. rows = (0,5) outputs array and corresponding row numbers
def detectRows(row,images,ras_data):
    geo,proj,output,shape,y_size,sample_size=ras_data
    r0,r1=row
    timeSeries,rownum=inputData(r0,r1,images,ras_data)
    ccdArray=[[((CCD(timeSeries[x][y],(rownum[x],y))))for y in range(y_size)]for x in range(np.shape(timeSeries)[0])]
    return ccdArray, rownum

#generate input rows for multiprocessing, input the # of rows to be processed by a given pool at a time
def rowTuples(num,ras_data,size):
    geo,proj,output,shape,y_size,sample_size=ras_data
    rows=shape[1]
    div=rows//num
    remain=shape[1]%num
    tuples=[(num*k,(num*(k+1)))for k in range(div)]
    divR=remain//size
    for k in range(divR):
        tuples.append(((div*num),(div*num)+remain))
    return tuples

def getDate(fusion_tif):
    gordinal = date(int(fusion_tif[-14:-10]), int(fusion_tif[-9:-7]), int(fusion_tif[-6:-4])).toordinal()
    return gordinal

def csvParameters(ras_data,size,nth,num,parent_dir):
    geo,proj,output,shape,y_size,sample_size=ras_data
    print("saving parameters to CSV")
    with open(str(output)+"/CCD_Parameters.csv", 'w') as f:
    #create the csv writer
        writer = csv.writer(f)
        writer.writerow(("date",date.today()))
        for k in dfs:
            row=dfs[k]
            writer.writerow((k, row))
        writer.writerow(("pool_size:",size))
        writer.writerow(("num of rows processed per pool:",num))
        writer.writerow(("pool_size:",size))
        writer.writerow(("nth images processed:",nth))
        writer.writerow(("parent directory:", parent_dir))
        writer.writerow(("output directory:", output))
        writer.writerow(("resample size:", sample_size))
        writer.writerow(("Y_size:", y_size))

def pixelCoordinates(ras_data):
    geo,proj,output,shape,y_size,sample_size=ras_data
    bands,rows,columns=shape
    array1=[[float(x+y/100000)for y in range(columns)]for x in range(rows)]
    nparray=np.array(array1)
    save_raster([nparray],"Pixel Coordinates"+str(sample_size)+'m',ras_data)


def csvResults(result_map,ras_data):
    geo,proj,output,shape,y_size,sample_size=ras_data
    with open(str(output)+"/CCD_resultsDict.csv", 'w') as f:
        #create the csv writer
        writer = csv.writer(f)
        for r in range(len(result_map)):
            for x in range(len(result_map[r][0])):
                row=result_map[r][1][x]
                for y in range(len(result_map[r][0][x])):
                    for seq in result_map[r][0][x][y]:
                     # open the file in the write mode
                     # write a row to the csv file
                        seq['pixel']=str((row,y))
                writer.writerows(result_map[r][0][x])
    #close the file
    f.close()
    print("results saved")

def main():
    
    start=time.time()
    #####Input Parameters#######

    #Directory of fusion stack
    parent_dir= dfs['parent_dir']

    #pool size
    size=dfs["pool_size"]

    #rows per pool
    num=dfs['num_rows']

    #resample pixel size(m)
    sample_size=dfs['resampleResolution']

    #nth images to use in stack
    nth=dfs['nth']
    

    #output_directory
    #output=parent_dir+str('/CCD_Output'+str(sample_size)+"m_resampledTrial2")
   
    #########CCD##############
    images,ras_data=loadImages(parent_dir,sample_size,nth)
    geo,proj,output,shape,y_size,sample_size=ras_data
    allFiles = glob.glob(os.path.join(parent_dir, '*.tif'))
    day1=getDate(images[0])
    day2=getDate(images[-1])
  

    #create ouptut directory if it dosen't exist
    if not os.path.isdir(output):
        os.mkdir(output)
    print(output)
    resample(images,ras_data)
    csvParameters(ras_data,size,nth,num,parent_dir,)
    tuples=rowTuples(num,ras_data,size)
    bands=["blue","green","red","nir","ndvi","ndwi"]
    p = multiprocessing.Pool(size)
    pixelCoordinates(ras_data)
    result_map = p.map(partial(detectRows,ras_data=ras_data,images=images),tuples)
    csvResults(result_map,ras_data)
    changeArray(result_map,day1+15,day2-90,ras_data)
    days=[(day1+15),(day2-90)]
    for day in days:
        rmse=[toArray(result_map, str(band),"rmse",day,ras_data)for band in bands]
        coefficients=[toArray(result_map,str(band),"coefficients",day,ras_data,k)for k in range(3)for band in bands]
        resampleImage(allFiles,day,ras_data)
        for ras in coefficients:
            rmse.append(ras)
        save_raster(rmse,"training_"+str(date.fromordinal(day)),ras_data)
        rmse.clear()
        coefficients.clear()
    end=time.time()-start
    print(end) 

if __name__ == '__main__':
    main()

