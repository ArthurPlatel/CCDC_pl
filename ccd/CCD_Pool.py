#from ccd.data_input import Allto_list
from typing import final
import ccd.FusionFunctions as FF
from parameters import defaults as dfs
import numpy as np
import time
from osgeo import gdal
from datetime import date
from ccd import detect
from collections import defaultdict
import multiprocessing
from functools import partial
import csv

#Function to ingetst time seties data by row from a Fusion image stack
def rowData(r0,r1,imageCollection):
    n=0
    shape = FF.shape(imageCollection)
    #time_series=np.zeros((shape[2],7,1))
    time_series=[[[[]for r in range(7)]for y in range(shape[2])]for x in range(r1-r0)]
    for fusion_tif in imageCollection:
        start_time = time.time()
        n+=1
        image=gdal.Open(fusion_tif)
        gordinal = date(int(fusion_tif[-14:-10]), int(fusion_tif[-9:-7]), int(fusion_tif[-6:-4])).toordinal()
        b_ras = image.GetRasterBand(1).ReadAsArray().astype(np.uint16)
        g_ras = image.GetRasterBand(2).ReadAsArray().astype(np.uint16)
        r_ras = image.GetRasterBand(3).ReadAsArray().astype(np.uint16)
        n_ras = image.GetRasterBand(4).ReadAsArray().astype(np.uint16)
        for x in range(r1-r0):
            for y in range(shape[2]):
                time_series[x][y][0].append(gordinal)
                time_series[x][y][1].append(b_ras[x+r0,y]) 
                time_series[x][y][2].append(g_ras[x+r0,y])  
                time_series[x][y][3].append(r_ras[x+r0,y])  
                time_series[x][y][4].append(n_ras[x+r0,y])    
                time_series[x][y][5].append((((n_ras[x+r0,y]-r_ras[x+r0,y])/(n_ras[x+r0,y]+r_ras[x+r0,y])*1000)))
                time_series[x][y][6].append(1)
        image_time=time.time()-start_time
        if n%100==0:
            print("image {} of {} completed in: {} \n".format(n, len(imageCollection),image_time))
    return time_series 
pix=0
def CCD(pixel_data):
    global pix 
    print("Finished ccding",pix)   
    pix+=1
    data=np.array(pixel_data)
    dates,blues,greens,reds,nirs,ndvis,qas=data
    result=detect(dates, greens,blues, reds, nirs, ndvis, qas, dfs['params'])
    return result["change_models"]
  
#CCD analysis by row  
def CCD_row(r1,factor,parent_dir):
    now=time.time()
    r0=r1-factor
    image_collection=FF.sortImages(parent_dir,True)
    print("processing row:",r0)
    rows=rowData(r0,r1,image_collection)
    rank = multiprocessing.current_process()
    print(rank)
    new=[[((CCD(rows[x][y])))for y in range(np.shape(rows)[1])]for x in range(np.shape(rows)[0])]
    #new=[[((CCD(rows[x][y])))for y in range(5)]for x in range(np.shape(rows)[0])]
    print("processed in {}".format(time.time()-now))
    return new


#Create output array filtered by date
def toArray(result_map,band,data,num,shape,day):
    emptyArray=np.zeros(shape[1:3])
    for x in range(len(result_map)):
        r=(x*len(result_map[x]))
        for l in range(len(result_map[x])):
            row=r+l
            for y in range(len(result_map[x][l])):
                 for seq in (result_map[x][l][y]):
                     if seq["start_day"]<=day<=seq["end_day"]:
                        if data=="coefficients":
                            emptyArray[row][y]=seq[band][data][num]
                        else:
                            emptyArray[row][y]=seq[band][data]
    return emptyArray



def toDF(seq):
    pixel=pd.DataFrame({
            #'RMSE blue':[seq["blue"]["rmse"]],
            #'RMSE green':[seq['green']['rmse']],
            'RMSE red':[seq['red']['rmse']],
            'RMSE nir':[seq['nir']['rmse']],
            'RMSE ndvi':[seq['ndvi']['rmse']],
            #'Coef1 blue':[seq['blue']["coefficients"][0]],
            #'Coef1 green':[seq['green']["coefficients"][0]],
            'Coef1 red':[seq['red']["coefficients"][0]],
            'Coef1 nir':[seq['nir']["coefficients"][0]],
            'Coef1 ndvi':[seq['ndvi']["coefficients"][0]],
        })
    return pixel

import pandas as pd
def csvResults(result_map,shape,out_dir):
    emptyArray=np.zeros(shape[1:3])
    dataFrames=pd.DataFrame([])
    field_names = ['start_day', 'end_day', 'break_day','observation_count','change_probability','curve_qa','blue','green','red','nir','ndvi','pixel']
    with open(str(out_dir)+"CCD_resultsDict.csv", 'w') as f:
        #create the csv writer
        writer = csv.writer(f)
        for x in range(len(result_map)):
            r=(x*len(result_map[x]))
            for l in range(len(result_map[x])):
                row=r+l
                for y in range(len(result_map[x][l])):
                    for seq in (result_map[x][l][y]):
                     # open the file in the write mode
                     # write a row to the csv file
                        seq['pixel']=str((row,y))
                writer.writerows(result_map[x][l])
    #close the file
    f.close()


#Creates an array based on pixel breakpoints to identify change between two dates
#To work on, Filter function is not working properly
def changeMap(result_map,shape,day1,day2,out_dir,geo,proj):
    emptyArray=np.zeros(shape[1:3])
    for x in range(len(result_map)):
        r=(x*len(result_map[x]))
        for l in range(len(result_map[x])):
            row=r+l
            for y in range(len(result_map[x][l])):
                 for seq in (result_map[x][l][y]):
                     if day1<=seq["break_day"]<day2:
                         emptyArray[row][y]=seq['break_day']
    datename= str(date.fromordinal(day1))+'_'+str(date.fromordinal(day2))
    save_raster(1,[emptyArray],shape,(str(datename)+"ChangeMap"),out_dir,geo,proj)


#generates a an array making it easy to identify individual pixel coordinates
def pixelCoordinates(shape):
    print(shape)
    bands,rows,columns=shape
    array1=[[float(x+y/10000)for y in range(columns)]for x in range(rows)]
    nparray=np.array(array1)
    return nparray

#function to save raster to output location set in parameters.py
def save_raster(band_count, arrays,shape,filename,out_dir,geo,proj,format='GTiff', dtype = gdal.GDT_Float32):
        bands,rows,cols=shape
        tot_path=str(out_dir)+str(filename)+".tif"
        # Initialize driver & create file
        driver = gdal.GetDriverByName(format)
        Image_out = driver.Create(tot_path,cols,rows,band_count,dtype)
        Image_out.SetGeoTransform(geo)
        Image_out.SetProjection(proj)
        for k in range(band_count):
            Image_out.GetRasterBand(k+1).WriteArray(arrays[k])
        print("Rasters Saved")
        Image_out_out = None

#create training rasters to train CCDC classifier
def trainingRaster(result_map,shape,day,out_dir,geo,proj):
    trainingArrays=[]
    bands = ['blue','green','red','nir','ndvi']
    for band in bands:
        trainingArrays.append(toArray(result_map,"blue","rmse",0,shape,day))
    for band in bands:
        # if band=='ndvi':
        #         trainingArrays.append(toArray(result_map,band,"coefficients",0,shape,day))
        for k in range(3):
            trainingArrays.append(toArray(result_map,band,"coefficients",k,shape,day))
    print("saving training rasters")
    save_raster(len(trainingArrays),trainingArrays,shape,str(date.fromordinal(day))+"training",out_dir,geo,proj)

#function to extract tif string into image date    
def getDate(fusion_tif):
    gordinal = date(int(fusion_tif[-14:-10]), int(fusion_tif[-9:-7]), int(fusion_tif[-6:-4])).toordinal()
    return gordinal

def imageCCD(parent_dir, out_dir,size=4,odd=True):
    now=time.time()
    sorted=FF.sortImages(parent_dir,odd)
    image=gdal.Open(sorted[0],True)
    geo=image.GetGeoTransform()
    proj=image.GetProjection()
    shape=FF.shape(sorted)
    day1=getDate(sorted[0])
    day2=getDate(sorted[len(sorted)-1])
    lines=[]
    for k in range(5,(shape[1]-(shape[1]%5))+5,5):
         lines.append(k)
    #lines=[670,675,680,685]
    fac=lines[1]-lines[0]
    pixels=pixelCoordinates(shape)
    save_raster(1,[pixels],shape,"_pixelCoordinates.tif",out_dir,geo,proj)
    p = multiprocessing.Pool(size)
    result_map = p.map(partial(CCD_row,factor=fac,parent_dir=parent_dir), lines)
    changeMap(result_map,shape,day1,day2,out_dir,geo,proj)
    trainingRaster(result_map,shape,(day1+30),out_dir,geo,proj)
    trainingRaster(result_map,shape,(day2-3),out_dir,geo,proj)
    csvResults(result_map,shape,out_dir)
    time_final=time.time()-now
    print("total process finished in:", time_final)

#def main():
    # parent_dir='/Users/arthur.platel/Desktop/Fusion_Images/CZU_FireV2'
    # out_dir= "/Users/arthur.platel/Desktop/CCDC_Output/CZU_FireV2/HighRows/"
    # imageCCD(parent_dir,out_dir)

    # now=time.time()
    # sorted=FF.sortImages(dfs["parent_dir"],True)
    # shape=FF.shape(sorted)
    # day1=getDate(sorted[0])
    # day2=getDate(sorted[len(sorted)-1])
    # lines=[]
    # for k in range(5,685,5):
    #      lines.append(k)
    # fac=lines[1]-lines[0]
    # size=4
    # pixels=pixelCoordinates(shape)
    # save_raster(1,[pixels],shape,"_pixelCoordinates.tif")
    # p = multiprocessing.Pool(size)
    # result_map = p.map(partial(CCD_row,factor=fac), lines)
    # changeMap(result_map,shape,day1,day2)
    # trainingRaster(result_map,shape,day=day1+30)
    # trainingRaster(result_map,shape,day=(day2-3))
    # csvResults(result_map,shape)
    # time_final=time.time()-now
    # print("total process finished in:", time_final)



# if __name__ == '__main__':
#     main()



