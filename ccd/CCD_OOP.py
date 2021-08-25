from ccd.CCD_master import changeArray, csvParameters, resampleImage
from osgeo import gdal
import glob
import numpy as np
import os
from datetime import date
from datetime import time
import time
from ccd import detect
from parameters2 import defaults as dfs
import multiprocessing
from functools import partial
import csv
import pandas as pd


def main():
    #parent_dir=str(input("Fusion Stack Directory?"))
    #sample_size=int(input("Resample Image Resolution Size (30m)?"))
    parent_dir='/Users/arthur.platel/Desktop/Fusion_Images/Konza/PF-SR'
    sample_size=30
    data=CCD(parent_dir,sample_size)
    #data.detectRows((0,5))
   
    
    
   

    

class CCD:
    def __init__(self, parent_dir=None, sample_size=3):
        self.parent_dir=parent_dir#pool size
        self.size=dfs['pool_size']
        self.num=dfs['num_rows']
        self.sample_size=sample_size
        self.nth=dfs['nth']
        self.output=self.parent_dir+str('/CCD_Output_'+str(self.sample_size)+'m_resampled_Trial')
        self.days=[]
        self.shape=0
        self.proj=0
        self.geo=0
        self.y_size=0
        self.images=self.setVariables()
        self.tuples=self.rowTuples()
        
        ############CCD################
        ###############################
        
        #create output directory
        if not os.path.isdir(self.output):
            os.mkdir(self.output)

        #create before and after tif, sampled at desired resoltuion
        if self.sample_size!=3:
            for k in range(len(self.days)):
                self.resampleImage(self.days[k])

        # Create CSV with CCD Parameters and save in output dir
        self.csvParameters()
        #output pixel coordinates raster
        self.pixelCoordinates()

        p = multiprocessing.Pool(self.size)
        result_map = p.map(self.detectRows,self.tuples)
        self.changeArray(result_map)
        self.trainingRasters(result_map)
        

    #/////////////////////#
    #####CCD Funtions######
    #/////////////////////#

    def setVariables(self):
        allFiles = glob.glob(os.path.join(self.parent_dir, '*.tif'))
        #Function to only use every nth image instead of the whole stack, set in parameters
        files=[]
        for k in range(len(allFiles)):
                    if k%self.nth==0:
                        files.append(allFiles[k])
        image0=gdal.Open(files[0])

        #Sort files chronologically assuming file name contains image date
        sortedFiles=sorted(files)

        #resample image to get shape
        if self.sample_size!=3:
            resampled=gdal.Warp('/vsimem/in_memory_output.tif',image0,xRes=self.sample_size,yRes=self.sample_size,resampleAlg=gdal.GRA_Average)
        else:
            resampled=image0

        #Global get info for output files
        self.geo=resampled.GetGeoTransform()
        self.proj=resampled.GetProjection()

        #global shape of image
        self.shape=np.shape(resampled.ReadAsArray())
        self.y_size=10
        print("Image shape {} at {} meter resolution".format(self.shape,self.sample_size))
        
        # set start/end dates of images
        self.days.append(self.getDate(sortedFiles[0]))
        self.days.append(self.getDate(sortedFiles[-1]))
        
        #resturn sorted filenames
        return sortedFiles
    
    #create list of processing rows for multiprocessing
    def rowTuples(self):
        rows=self.shape[1]
        div=rows//self.num
        remain=self.shape[1]%self.num
        tuples=[(self.num*k,(self.num*(k+1)))for k in range(div)]
        divR=remain//self.size
        for k in range(divR):
            tuples.append(((div*self.num),(div*self.num)+remain))
        return tuples

    def getDate(self,fusion_tif):
        gordinal = date(int(fusion_tif[-14:-10]), int(fusion_tif[-9:-7]), int(fusion_tif[-6:-4])).toordinal()
        return gordinal

    def resampleImage(self,day):
        for image in self.images:
            days = date(int(image[-14:-10]), int(image[-9:-7]), int(image[-6:-4])).toordinal()
            if int(day)==int(days):
                image0=gdal.Open(image)
                if self.sample_size!=3:
                    image0=gdal.Warp(self.output+"/"+str(date.fromordinal(day))+'_'+str(self.sample_size)+'m.tif',image0,xRes=self.sample_size,yRes=self.sample_size,resampleAlg=gdal.GRA_Average)

    def csvParameters(self):
        print("saving parameters to CSV")
        with open(str(self.output)+"/CCD_Parameters.csv", 'w') as f:
        #create the csv writer
            writer = csv.writer(f)
            writer.writerow(("date",date.today()))
            writer.writerow(("time started:",str(time.localtime(time.time()).tm_hour) +':'+str(time.localtime(time.time()).tm_min)))
            writer.writerow(("parent directory:", self.parent_dir))
            writer.writerow(("output directory:", self.output))
            writer.writerow(("resample size:", self.sample_size))
            for k in dfs:
                row=dfs[k]
                writer.writerow((k, row))

    def save_raster(self,arrays,name,format='GTiff', dtype = gdal.GDT_Float32):
        #output variables
        band,rows,cols=self.shape
        band_count=len(arrays)
        tot_path=str(self.output)+"/"+str(name)+".tif"
        # Initialize driver & create file
        driver = gdal.GetDriverByName(format)
        Image_out = driver.Create(tot_path,cols,rows,band_count,dtype)
        Image_out.SetGeoTransform(self.geo)
        Image_out.SetProjection(self.proj)
        for k in range(band_count):
            Image_out.GetRasterBand(k+1).WriteArray(arrays[k])
        print("{} Rasters Saved".format(name))
        Image_out= None

    def pixelCoordinates(self):
        bands,rows,columns=self.shape
        array1=[[float(x+y/100000)for y in range(columns)]for x in range(rows)]
        nparray=np.array(array1)
        self.save_raster([nparray],"Pixel_Coordinates"+str(self.sample_size)+'m')

    def inputData(self,r0,r1):
        #save row numbers
        rows=[]
        numRows=r1-r0
        for k in range(r0,r1):
            rows.append(k)
    
        #create empty array to store time series data for entire image stack
        time_series=[[[[]for r in range(8)]for y in range(self.y_size)]for x in range(numRows)]
        
        # Open every Fusion tif, resample to desired size and extract pixel values for each band into "time_series" array
        #returns array with all pixel values from time series
        print("collecting pixel data from {} images for rows {} to {} using {}".format(len(self.images),r0,r1-1,str(multiprocessing.current_process())[-42:-30]))
        imageTime=time.time()
        for fusion_tif in self.images:
            day = date(int(fusion_tif[-14:-10]), int(fusion_tif[-9:-7]), int(fusion_tif[-6:-4]))
            image0=gdal.Open(fusion_tif)
            gordinal = day.toordinal()
            if self.sample_size!=3:
                image=gdal.Warp('/vsimem/in_memory_output',image0,xRes=self.sample_size,yRes=self.sample_size,resampleAlg=gdal.GRA_Average)
            else:
                image=image0
            blue = image.GetRasterBand(1).ReadAsArray()
            green = image.GetRasterBand(2).ReadAsArray()
            red = image.GetRasterBand(3).ReadAsArray()
            nir = image.GetRasterBand(4).ReadAsArray()
            for l in range(numRows):
                x=rows[l]
                for y in range(self.y_size):
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
        
    def detectRows(self,row):
        r0,r1=row
        #####
        #save row numbers for stic
        rows=[]
        numRows=r1-r0
        for k in range(r0,r1):
            rows.append(k)
 
        #create empty array to store time series data for entire image stack
        time_series=[[[[]for r in range(8)]for y in range(self.y_size)]for x in range(numRows)]
        
        # Open every Fusion tif, resample to desired size and extract pixel values for each band into "time_series" array
        #returns array with all pixel values from time series
        print("collecting pixel data from {} images for rows {} to {} using {}".format(len(self.images),r0,r1-1,str(multiprocessing.current_process())[-42:-30]))
        imageTime=time.time()
        for fusion_tif in self.images:
            day = date(int(fusion_tif[-14:-10]), int(fusion_tif[-9:-7]), int(fusion_tif[-6:-4]))
            image0=gdal.Open(fusion_tif)
            gordinal = day.toordinal()
            if self.sample_size!=3:
                image=gdal.Warp('/vsimem/in_memory_output',image0,xRes=self.sample_size,yRes=self.sample_size,resampleAlg=gdal.GRA_Average)
            else:
                image=image0
            blue = image.GetRasterBand(1).ReadAsArray()
            green = image.GetRasterBand(2).ReadAsArray()
            red = image.GetRasterBand(3).ReadAsArray()
            nir = image.GetRasterBand(4).ReadAsArray()
            for l in range(numRows):
                x=rows[l]
                for y in range(self.y_size):
                    time_series[l][y][0].append(gordinal)
                    time_series[l][y][1].append(blue[x,y]) 
                    time_series[l][y][2].append(green[x,y])  
                    time_series[l][y][3].append(red[x,y])  
                    time_series[l][y][4].append(nir[x,y])    
                    time_series[l][y][5].append(((nir[x,y]-red[x,y])/(nir[x,y]+red[x,y]))*1000)
                    time_series[l][y][6].append(((green[x,y]-nir[x,y])/(green[x,y]+nir[x,y]))*1000)
                    time_series[l][y][7].append(1)
        print("{} completed ingesting images in {}".format(str(multiprocessing.current_process())[-42:-30],time.time()-imageTime))
        ########
        print("ccding")
        ccdArray=[[((self.CCD_main(time_series[x][y],(rows[x],y))))for y in range(self.y_size)]for x in range(np.shape(time_series)[0])]
        return ccdArray, rows

    def csvResults(self,result_map):
        with open(str(self.output)+"/CCD_resultsDict.csv", 'w') as f:
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

    ###### Creates a change map between first and last date of images 
    def changeArray(self,result_map):
        day1=self.days[0]+15
        day2=self.days[1]-45
        emptyArray=np.zeros((self.shape[1],self.y_size))
        for r in range(len(result_map)):
            for x in range(len(result_map[r][0])):
                row=result_map[r][1][x]
                for y in range(len(result_map[r][0][x])):
                    for seq in result_map[r][0][x][y]:
                        if day1<=seq["end_day"]<day2:
                            emptyArray[row,y]=seq["break_day"]
        self.save_raster([emptyArray],"Change_"+str(date.fromordinal(day1))+'_To_'+str(date.fromordinal(day2)))
        

    ##### MAIN CCD function from USGS PY-CCD#######    
    def CCD_main(self,pixel_data,pixel_coordinates):
        now=time.time()
        data=np.array(pixel_data)
        dates,blues,greens,reds,nirs,ndvis,ndwis,qas=data
        result=detect(dates, greens,blues, reds, nirs, ndvis, ndwis, qas, dfs['params'])
        final_time=time.time()-now
        print("processing pixel {}".format(pixel_coordinates))
        return result["change_models"]


    def toArray(self,result_map,band,dtp,day,k=None):
        emptyArray=np.zeros((self.shape[1],self.y_size))
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


    #### Creates training rasters for classification
    def trainingRasters(self,result_map):
        for day in self.days:
            outArray=[self.toArray(result_map, str(band),"rmse",day,)for band in dfs['CCD_bands']]
            coefficients=[self.toArray(result_map,str(band),"coefficients",day,k)for k in range(3)for band in dfs['CCD_bands']]
            self.resampleImage(day)
        for ras in coefficients:
            outArray.append(ras)
        self.save_raster(outArray,"training_"+str(date.fromordinal(day)))
        outArray.clear()
        coefficients.clear()

if __name__ == '__main__':
    main()
