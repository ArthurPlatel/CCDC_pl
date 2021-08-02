import numpy as np
from osgeo import gdal
import glob, os
from datetime import date
import time
import ccd

params = {'QA_BITPACKED': False,
              'QA_FILL': 255,
              'QA_CLEAR': 0,
              'QA_WATER': 1,
              'QA_SHADOW': 2,
              'QA_SNOW': 3,
              'QA_CLOUD': 4
              }

#parent_dir="/Users/arthur.platel/Desktop/Fusion_Images/Imperial_Subset"
parent_dir = '/Users/arthur.platel/desktop/Fusion_Images/CZU/PF-SR'
out_dir = "/Users/arthur.platel/Desktop/CCDC_Output/line803Trial"

files = glob.glob(os.path.join(parent_dir, '*.tif'))
final = sorted(files)
image=gdal.Open(final[0])
shape=image.ReadAsArray().shape
driver = image.GetDriver()
x_pixels=shape[1]
y_pixels=shape[2]
output={}

def to_dict(r0,r1,x_pixels,final):
        time_series={(x,y):{'dates':[],'blue':[],'green':[], 'red':[], 'nir':[],'ndvi':[],'qas':[]} for y in range(r0,r1) for x in range(x_pixels)} 
        n=0
        t=0
        for fusion_tif in final:
            start_time = time.time()
            if n%100==0:
                print("processing image {} of {}".format(n, len(final)-1))
            image=gdal.Open(fusion_tif)
            gordinal = date(int(fusion_tif[-14:-10]), int(fusion_tif[-9:-7]), int(fusion_tif[-6:-4])).toordinal()
            b_ras = image.GetRasterBand(1).ReadAsArray()
            g_ras = image.GetRasterBand(2).ReadAsArray()
            r_ras = image.GetRasterBand(3).ReadAsArray()
            n_ras = image.GetRasterBand(4).ReadAsArray()
            for x in range(x_pixels):
                for y in range(r0,r1):
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



def save_raster(path,time_num, band_count, arrays, image,string,format='GTiff', dtype = gdal.GDT_Float32):
    rows,cols = arrays[0].shape
    name= date.fromordinal(time_num)
    tot_path=path+str(name)+str(string)+".tif"
    # Initialize driver & create file
    driver = gdal.GetDriverByName(format)
    Image_out = driver.Create(tot_path,cols,rows,band_count,dtype)
    Image_out.SetGeoTransform(image.GetGeoTransform())
    Image_out.SetProjection(image.GetProjection())
    for k in range(band_count):
        Image_out.GetRasterBand(k+1).WriteArray(arrays[k])
    Image_out_out = None


def pixel_ccd(rx,ry,x_pixels,final,day,emptyArray,bands):
    dict=to_dict(rx,ry,x_pixels,final)
    n=0
    for k in dict:
        n+=1
        result_time=time.time()
        d=dict[k]
        data = np.array([d['dates'],d['blue'],d["green"],d['red'],d['nir'],d['ndvi'],d['qas']])
        dates,blues,greens,reds,nirs,ndvis,qas = data
        print(dates)
        print(blues)
        result=ccd.detect(dates, greens,blues, reds, nirs, ndvis, qas, params)
        for sequence in result['change_models']:
            print("pixel {} has {}".format(k,sequence))
            # pixel=False
            # if sequence["start_day"]<=day<=sequence["end_day"]:
            #     print("pixel: {} has SD{},BD {},ED:{}".format(k,sequence["start_day"],sequence['break_day'],sequence["end_day"]))
            #     for band in range(len(bands)):
            #         for l in range(6):
            #             emptyArray[1][band][l][k]=sequence[bands[band]]["coefficients"][l]
            #         emptyArray[0][band][k]=sequence[bands[band]]["rmse"]
                    

#bands to produce values for
bands=['blue','green','red','nir','ndvi']
#create empty arrays for each raster to output
empty_array=[[np.empty((shape[1],shape[2]))for l in range(len(bands))],[[np.empty((shape[1],shape[2]))for y in range(6)] for x in range(len(bands))]]
now=time.time()
for k in range(802,804): 
    print("processing line: {}".format(k))
    pixel_ccd(k,k+1,x_pixels,final,737971,empty_array,bands)
    save_raster(out_dir,737971,len(empty_array[0]),empty_array[0],image,"_RMSE")
    for k in range(len(bands)):
        save_raster(out_dir,737971,len(empty_array[1][k]),empty_array[1][k],image,str(bands[k])+"_coefs")
saveTime=time.time()-now
print("files saved, total processing completed in {}".format(saveTime))

#     for k in range(len(bands)):

#         save_raster(out_dir,736696,eA,image)







