import csv
from math import nan
import pandas as pd
from pandas.core import arrays
from Build_RFC import createClassifier
from Build_RFC import classify
from datetime import date
import numpy as np
from pandas.core.dtypes.missing import notna
from CCD_master import save_raster
from osgeo import gdal
import FusionFunctions as FF
from parameters import defaults as dfs
import os
from pathlib import Path
import glob
import math
import multiprocessing
from functools import partial
from ccd.CCD_master import csvParameters

def toDF(seq):
    pixel=pd.DataFrame({
            'RMSE blue':[seq["blue"]["rmse"]],
            'RMSE green':[seq['green']['rmse']],
            'RMSE red':[seq['red']['rmse']],
            'RMSE nir':[seq['nir']['rmse']],
            'RMSE ndvi':[seq['ndvi']['rmse']],
            'Coef1 blue':[seq['blue']["coefficients"][0]],
            'Coef2 blue':[seq['blue']["coefficients"][1]],
            'Coef3 blue':[seq['blue']["coefficients"][2]],
            'Coef1 green':[seq['green']["coefficients"][0]],
            'Coef2 green':[seq['green']["coefficients"][1]],
            'Coef3 green':[seq['green']["coefficients"][2]],
            'Coef1 red':[seq['red']["coefficients"][0]],
            'Coef2 red':[seq['red']["coefficients"][1]],
            'Coef3 red':[seq['red']["coefficients"][2]],
            'Coef1 nir':[seq['nir']["coefficients"][0]],
            'Coef2 nir':[seq['nir']["coefficients"][1]],
            'Coef3 nir':[seq['nir']["coefficients"][2]],
            'Coef1 ndvi':[seq['ndvi']["coefficients"][0]],
            'Coef2 ndvi':[seq['ndvi']["coefficients"][1]],
            'Coef3 ndvi':[seq['ndvi']["coefficients"][2]],
        })
    return pixel

CCD_csvFile=dfs['CCD_output_CSVfile']
sample_size=dfs["resampleResolution"]
nth=dfs['nth']
name=dfs["className"]
path=Path(CCD_csvFile)
dir=path.parent.absolute()
output=str(dir)+"/Classifications"
parent_dir=Path(dir).parent.absolute()
#parent_dir='/Users/arthur.platel/Desktop/Fusion_Images/deforestationV2/PF-SR'
allFiles = glob.glob(os.path.join(parent_dir, '*.tif'))
if not os.path.isdir(output):
    os.mkdir(output)
clsf=createClassifier()
files=[]
for k in range(len(allFiles)):
            if k%nth==0:
                files.append(allFiles[k])
image0=gdal.Open(files[0])
image=gdal.Warp('/vsimem/in_memory_output.tif',image0,xRes=sample_size,yRes=sample_size,sampleAlg='average')
geo=image.GetGeoTransform()
proj=image.GetProjection()
shape=np.shape(image.ReadAsArray())
y_size=shape[2]
ras_data=geo,proj,output,shape,y_size,sample_size
outarray1=np.zeros((shape[1],shape[2]))
outarray2=np.zeros((shape[1],shape[2]))
rasters=[outarray1,outarray2]
# with open(CCD_csvFile) as csv_file2:
#     csv_reader = csv.reader(csv_file2, delimiter=',')
#     r=0
#     for row in csv_reader:
#         print("classifiying pixel",r)
#         r+=1
#         if len(row)<=1:
#             df1=toDF(eval(row[0]))
#             df2=toDF(eval(row[0]))
#             class1=classify(df1,clsf)
#             class2=classify(df2,clsf)
#             px1=eval(eval(row[0])["pixel"])
#             rasters[0][px1[0]][px1[1]]=class1
#             rasters[1][px1[0]][px1[1]]=class2
#         else:
#             df1=toDF(eval(row[0]))
#             df2=toDF(eval(row[1]))
#             class1=classify(df1,clsf)
#             class2=classify(df2,clsf)
#             px1=eval(eval(row[0])["pixel"])
#             px2=eval(eval(row[1])["pixel"])
#             rasters[0][px1[0]][px1[1]]=class1
#             rasters[1][px2[0]][px2[1]]=class2
#         if r%100==0:
#             save_raster(rasters,name,ras_data)
# save_raster(rasters,name,ras_data)

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

with open(CCD_csvFile) as csv_file:
    read=pd.read_csv(csv_file, delimiter=',',header=None,names=list(range(5))).dropna(axis='columns',how='all')
    for k in range(len(read)):
        for l in range(0,1): 
            if pd.isnull(read[l][k])==False:
                print(eval(read[l][k])['pixel'])
                df1=toDF(eval(read[l][k]))
                class1=classify(df1,clsf)
                pix=eval(eval(read[l][k])['pixel'])
                print(pix[0])
                outarray1[pix[0],pix[1]]=class1
        for g in range(1,2):
            if pd.isnull(read[g][k])==False:
                pix=eval(eval(read[l][k])['pixel'])
                df2=toDF(eval(read[g][k]))
                class2=classify(df2,clsf)
                outarray2[pix[0],pix[1]]=class2
            else:
                pix=eval(eval(read[g-1][k])['pixel'])
                outarray2[pix[0],pix[1]]=outarray1[pix[0],pix[1]]
        if k%100==0:
            save_raster(rasters,name,ras_data)
save_raster(rasters,name,ras_data)


def read_csv(filename):
    'converts a filename to a pandas dataframe'
    return pd.read_csv(filename, delimiter=',',header=None,names=list(range(5))).dropna(axis='columns',how='all')

def classif(rows,CCD_csvFile,ras_data):
        geo,proj,output,shape,y_size,sample_size=ras_data
        outarray1=np.zeros((rows[1]-rows[0],shape[2]))
        outarray2=np.zeros((rows[1]-rows[0],shape[2]))
        rasters=[outarray1,outarray2]
        #outarray2=np.zeros((len(range(rows)),shape[2]))
        rasters=[outarray1,outarray2]
        print(rows[1]-rows[0])
        value1=shape[2]*rows[0]
        value2=shape[2]*rows[1]
        with open(CCD_csvFile) as csv_file:
            read=read_csv(csv_file)
            for k in range(value1,value2):
                for l in range(0,1): 
                    if pd.isnull(read[l][k])==False:
                        print(eval(read[l][k])['pixel'])
                        df1=toDF(eval(read[l][k]))
                        class1=classify(df1,clsf)
                        pix=eval(eval(read[l][k])['pixel'])
                        print(pix[0])
                        outarray1[pix[0],pix[1]]=class1
                for g in range(1,2):
                    if pd.isnull(read[g][k])==False:
                        pix=eval(eval(read[l][k])['pixel'])
                        df2=toDF(eval(read[g][k]))
                        class2=classify(df2,clsf)
                        outarray2[pix[0],pix[1]]=class2
                    else:
                        pix=eval(eval(read[g-1][k])['pixel'])
                        outarray2[pix[0],pix[1]]=outarray1[pix[0],pix[1]]
        return rasters,rows,shape
#         if k%100==0:
#             save_raster(rasters,name,ras_data)
# save_raster(rasters,name,ras_data)

def main():
    size=4
    tuples=rowTuples(5,ras_data,size)
    p = multiprocessing.Pool(size)
    result_map = p.map(partial(classif,CCD_csvFile=CCD_csvFile,ras_data=ras_data),tuples)
    print(multiprocessing.current_process())
    classArray1=result_map[0][0]
    classArray2=result_map[0][1]
    rows=result_map[1]
    for rowNum in len(classArray1):
        for y in len(classArray1[rowNum]):
            outarray1[rows[0]+rowNum,y]=classArray1[rowNum,y]
    for rowNum in len(classArray2):
        for y in len(classArray2[rowNum]):
            outarray2[rows[0]+rowNum,y]=classArray2[rowNum,y]
    save_raster(rasters,name,ras_data)

# if __name__ == '__main__':
#     main()

