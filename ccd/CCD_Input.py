import csv
import pandas as pd
import classification as cls
from datetime import date
import numpy as np
from CCD_Pool import save_raster
from osgeo import gdal
import FusionFunctions as FF
from parameters import defaults as dfs

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

csvFile='/Users/arthur.platel/Desktop/CCD_ResampleOutputs/CZU_Fire30m/CCD_resultsDict.csv'
clsf=cls.createClassifier()
classList=['redwood','barren','otherveg','burned']
parent_dir='/Users/arthur.platel/Desktop/Fusion_Images/CZU_FireV2'
sorted=FF.sortImages(parent_dir)
im=gdal.Open(sorted[0],True)
image=gdal.Translate('/vsimem/in_memory_output.tif',im,xRes=dfs['resampleSize'],yRes=dfs['resampleSize'])
geo=image.GetGeoTransform()
proj=image.GetProjection()
shape=np.shape(image.ReadAsArray())
outarray1=np.zeros((shape[1],shape[2]))
outarray2=np.zeros((shape[1],shape[2]))
rasters=[outarray1,outarray2]
with open(csvFile) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    r=0
    for row in csv_reader:
        print("classifiying pixel",r)
        r+=1
        if len(row)<=1:
            df1=toDF(eval(row[0]))
            df1=toDF(eval(row[0]))
            px1=eval(eval(row[0])["pixel"])
            rasters[0][px1[0]][px1[1]]=class1
            rasters[1][px1[0]][px1[1]]=class2
        else:
            df1=toDF(eval(row[0]))
            df2=toDF(eval(row[1]))
            class1=cls.classify(df1,clsf)
            class2=cls.classify(df2,clsf)
            px1=eval(eval(row[0])["pixel"])
            px2=eval(eval(row[1])["pixel"])
            rasters[0][px1[0]][px1[1]]=class1
            rasters[1][px2[0]][px2[1]]=class2
        if r%100==0:
            save_raster(2,rasters,shape,"FireClassUpdated",'/Users/arthur.platel/Desktop/CCD_ResampleOutputs/CZU_Fire30m/TrainingData2',geo,proj)
save_raster(2,rasters,shape,"FireClassUpdated",'/Users/arthur.platel/Desktop/CCD_ResampleOutputs/CZU_Fire30m/',geo,proj)

        # print("pixel {} was {} until {} when it was {}".format(eval(row[0])["pixel"],classList[int(class1)],date.fromordinal(eval(row[0])["break_day"]),classList[int(class2)]))


