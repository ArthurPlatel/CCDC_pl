import numpy as np
from osgeo import gdal
import glob, os
from datetime import date
import time
from ccd import detect
from parameters import defaults as dfs
pix=0


def pixelData(imageCollection):
    n=0
    shape=[4,686,976] #[4,641,689]
    time_series=[[[[]for r in range(7)]for y in range(shape[2])]for x in range(shape[1])]
    modulo=shape[1]%100
    end=shape[1]-modulo
    for fusion_tif in imageCollection:
        start_time = time.time()
        n+=1
        print("processing image {} of {}".format(n, len(imageCollection)))
        image=gdal.Open(fusion_tif)
        gordinal = date(int(fusion_tif[-14:-10]), int(fusion_tif[-9:-7]), int(fusion_tif[-6:-4])).toordinal()
        b_ras = image.GetRasterBand(1).ReadAsArray().astype(np.uint16)
        g_ras = image.GetRasterBand(2).ReadAsArray().astype(np.uint16)
        r_ras = image.GetRasterBand(3).ReadAsArray().astype(np.uint16)
        n_ras = image.GetRasterBand(4).ReadAsArray().astype(np.uint16)
        shape=image.ReadAsArray().shape
        for k in range(int(end/100)):
            for x in range(shape[1]):
                for y in range(k*100,(k+1)*100):
                    time_series[x][y][0].append(gordinal)
                    time_series[x][y][1].append(b_ras[x,y]) 
                    time_series[x][y][2].append(g_ras[x,y])  
                    time_series[x][y][3].append(r_ras[x,y])  
                    time_series[x][y][4].append(n_ras[x,y])    
                    time_series[x][y][5].append((n_ras[x,y]-r_ras[x,y])/(n_ras[x,y]+r_ras[x,y]))
                    time_series[x][y][6].append(1)
        for x in range(shape[1]):
            for y in range(end,shape[2]):
                    time_series[x][y][0].append(gordinal)
                    time_series[x][y][1].append(b_ras[x,y]) 
                    time_series[x][y][2].append(g_ras[x,y])  
                    time_series[x][y][3].append(r_ras[x,y])  
                    time_series[x][y][4].append(n_ras[x,y])    
                    time_series[x][y][5].append((n_ras[x,y]-r_ras[x,y])/(n_ras[x,y]+r_ras[x,y]))
                    time_series[x][y][6].append(1)
            image_time=time.time()-start_time
        print("image completed in: {} \n".format(image_time))
    return time_series 



def CCD(pixel_data):
    global pix 
    print("ccding pixel",pix)   
    pix+=1
    data=np.array(pixel_data)
    dates,blues,greens,reds,nirs,ndvis,qas=data
    result=detect(dates, greens,blues, reds, nirs, ndvis, qas, dfs['params'])
    return result

def detectAll(inputArray):
    now=time.time()
    new=[[[CCD(inputArray[x][y])]for y in range(len(inputArray[1]))] for x in range(len(inputArray))]
    print("completed ccding in:", time.time()-now)
    return new

def detectRow(inputArray,out_array, r0, r1):
    rows,cols,bands=np.shape(inputArray)
    array=[[(CCD(inputArray[x][y])) for y in range(cols)]for x in range(r0,r1)]
    #
    # for x in range(r0,r1):
    #     for y in range(cols):
    #         array[x][y]=(CCD(inputArray[x][y])["change_models"])
    #print(array)
    #print(out_array[r0])
    out_array[r0]=array[0]
    return array 
    