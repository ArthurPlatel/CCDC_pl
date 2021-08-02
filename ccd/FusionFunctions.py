import numpy as np
from osgeo import gdal
import glob, os
from datetime import date
import time
import ccd
#import classification as cls
from parameters import defaults as dfs



#parent_dir="/Users/arthur.platel/Desktop/Fusion_Images/Imperial_Subset"

#function to load images locally and sort by date
def sortImages(parent_dir,odd=False):
    files = glob.glob(os.path.join(parent_dir, '*.tif'))
    if odd==True:
        odd=[]
        for image in files:
            if (int(image[-5:-4])%2) != 0:
                odd.append(image)
        images = sorted(odd)
    else:
        images=sorted(files)
    return images

def shape(sortedImages):
    image=gdal.Open(sortedImages[0])
    shape=np.shape(image.ReadAsArray())
    return shape




def to_dict(c0,c1,x_pixels,images):
        time_series={(x,y):{'dates':[],'blue':[],'green':[], 'red':[], 'nir':[],'ndvi':[],'qas':[]} for y in range(c0,c1) for x in range(x_pixels)} 
        n=0
        t=0
        for fusion_tif in images:
            start_time = time.time()
            if n%100==0:
                print("processing image {} of {}".format(n, len(images)-1))
            image=gdal.Open(fusion_tif)
            gordinal = date(int(fusion_tif[-14:-10]), int(fusion_tif[-9:-7]), int(fusion_tif[-6:-4])).toordinal()
            b_ras = image.GetRasterBand(1).ReadAsArray().astype(np.uint16)
            g_ras = image.GetRasterBand(2).ReadAsArray().astype(np.uint16)
            r_ras = image.GetRasterBand(3).ReadAsArray().astype(np.uint16)
            n_ras = image.GetRasterBand(4).ReadAsArray().astype(np.uint16)
            for x in range(x_pixels):
                for y in range(c0,c1):
                    time_series[x,y]["dates"].append(gordinal)
                    time_series[x,y]["blue"].append(b_ras[x,y]) 
                    time_series[x,y]["green"].append(g_ras[x,y])  
                    time_series[x,y]["red"].append(r_ras[x,y])  
                    time_series[x,y]["nir"].append(n_ras[x,y])    
                    time_series[x,y]["ndvi"].append((n_ras[x,y]-r_ras[x,y])/(n_ras[x,y]+r_ras[x,y]))
                    time_series[x,y]["qas"].append(1) 
            image_time=time.time()-start_time
            t+=image_time
            if n%100==0:
                print("image completed in: {} for a total time of {} min \n".format(image_time,(t/3600)))
            n+=1
        return time_series



def save_raster(path,time_num, band_count, arrays, images,string,format='GTiff', dtype = gdal.GDT_Float32):
    rows,cols,bands = np.shape(arrays[0])
    image=gdal.Open(images[0])  
    name= date.fromordinal(time_num)
    tot_path=path+str(name)+str(string)+".tif"
    # Initialize driver & create file
    driver = gdal.GetDriverByName(format)
    Image_out = driver.Create(tot_path,cols,rows,band_count,dtype)
    Image_out.SetGeoTransform(image.GetGeoTransform())
    Image_out.SetProjection(image.GetProjection())
    for k in range(band_count):
        Image_out.GetRasterBand(k+1).WriteArray(arrays[k])
    print("Rasters Saved")
    Image_out_out = None


def save_raster2(path,band_count, arrays, images,string,format='GTiff', dtype = gdal.GDT_Float32):
    rows,cols=np.shape(arrays)
    image=gdal.Open(images[0]) 
    tot_path=path+str(string)+".tif"
    # Initialize driver & create file
    driver = gdal.GetDriverByName(format)
    Image_out = driver.Create(tot_path,cols,rows,band_count,dtype)
    Image_out.SetGeoTransform(image.GetGeoTransform())
    Image_out.SetProjection(image.GetProjection())
    for k in range(band_count):
        Image_out.GetRasterBand(k+1).WriteArray(arrays[k])
    print("Rasters Saved")
    Image_out_out = None


def toClassify(cx,cy,x_pixels,images,day,emptyArray,bands):
    dict=to_dict(cx,cy,x_pixels,images)
    n=0
    for k in dict:
        n+=1
        result_time=time.time()
        d=dict[k]
        data = np.array([d['dates'],d['blue'],d["green"],d['red'],d['nir'],d['ndvi'],d['qas']])
        dates,blues,greens,reds,nirs,ndvis,qas = data
        result=ccd.detect(dates, greens,blues, reds, nirs, ndvis, qas, dfs['params'])
        for sequence in result['change_models']:
            # pixel=False
            if sequence["start_day"]<=day<=sequence["end_day"]:
                print("pixel: {} has SD{},BD {},ED:{}".format(k,sequence["start_day"],sequence['break_day'],sequence["end_day"]))
                for band in range(len(bands)):
                    for l in range(6):
                        emptyArray[1][band][l][k]=sequence[bands[band]]["coefficients"][l]
                    emptyArray[0][band][k]=sequence[bands[band]]["rmse"]
                    

def classification_ccd(cx,cy,x_pixels,images,array,day1,day2):
    dict=to_dict(cx,cy,x_pixels,images)
    n=0
    for k in dict:
        if k[0]%50==0:
            print("processing pixel {} of {}".format(k[0],len(dict)))
        n+=1
        result_time=time.time()
        d=dict[k]
        data = np.array([d['dates'],d['blue'],d["green"],d['red'],d['nir'],d['ndvi'],d['qas']])
        dates,blues,greens,reds,nirs,ndvis,qas = data
        result=ccd.detect(dates, greens,blues, reds, nirs, ndvis, qas, dfs['params'])       
        for seq in result["change_models"]:
            if seq["start_day"]<=day1<=seq["end_day"]:
                pixel_df= cls.toDF(seq)
                class_pred=cls.classify(pixel_df)
                seq['landclass']=class_pred
                array[0][k]=class_pred
            elif seq["start_day"]<=day2<=seq["end_day"]:
                pixel_df= cls.toDF(seq)
                class_pred=cls.classify(pixel_df)
                seq['landclass']=class_pred
                array[1][k]=class_pred

# class_array=np.empty((shape[1],shape[2]))
# final_dict=classification_ccd(0,1,x_pixels,images,class_array)
# print(final_dict)
#save_raster(out_dir,737971,1,class_array,image,"class")
        # for sequence in result['change_models']:
        #     if sequence["start_day"]<=day<=sequence["end_day"]:
        #         print("pixel: {} has SD{},BD {},ED:{}".format(k,sequence["start_day"],sequence['break_day'],sequence["end_day"]))
        #         for band in range(len(bands)):
        #             for l in range(6):
        #                 emptyArray[1][band][l][k]=sequence[bands[band]]["coefficients"][l]
        #             emptyArray[0][band][k]=sequence[bands[band]]["rmse"]


# #bands to produce values for
# bands=['blue','green','red','nir','ndvi']
# #create empty arrays for each raster to output
# empty_array=[[np.empty((shape[1],shape[2]))for l in range(len(bands))],[[np.empty((shape[1],shape[2]))for y in range(6)] for x in range(len(bands))]]
# now=time.time()
# for k in range(802,804): 
#     print("processing line: {}".format(k))
#     toClassify(k,k+1,x_pixels,images,737971,empty_array,bands)
#     save_raster(out_dir,737971,len(empty_array[0]),empty_array[0],image,"_RMSE")
#     for k in range(len(bands)):
#         save_raster(out_dir,737971,len(empty_array[1][k]),empty_array[1][k],image,str(bands[k])+"_coefs")
# saveTime=time.time()-now
# print("files saved, total processing completed in {}".format(saveTime))

# #     for k in range(len(bands)):

# #         save_raster(out_dir,736696,eA,image)







