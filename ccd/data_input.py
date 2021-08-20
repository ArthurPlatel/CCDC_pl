import numpy as np
from osgeo import gdal
import glob, os
from datetime import date
import time
from ccd import detect
import ccd.FusionFunctions as FF
from ccd.parameters import defaults as dfs





def get_data(parent_dir,pixel_x,pixel_y,sampleSize,odd=False):
    #initialize empty lists
    o_time = []
    greens = []
    blues = []
    reds = []
    nirs = []
    ndvis = []
    ndwis = []
    qas = []
    qa = 0
    #files = glob.glob(os.path.join(parent_dir, '*.tif'))
    #final = sorted(files)
    final= FF.sortImages(parent_dir,odd)
    for fusion_tif in final:
        image0=gdal.Open(fusion_tif)
        if sampleSize!=3:
            image=gdal.Warp('/vsimem/in_memory_output.tif',image0,xRes=sampleSize,yRes=sampleSize,resampleAlg=gdal.GRA_Average)
        else:
            image=image0
        year = int(fusion_tif[-14:-10])
        month = int(fusion_tif[-9:-7])
        day = int(fusion_tif[-6:-4])
        o_date = date(year, month, day)
        gordinal = o_date.toordinal()
        o_time.append(gordinal)
        #arrayLen = len(image.GetRasterBand(1).ReadAsArray())
        blue = float(image.GetRasterBand(1).ReadAsArray()[pixel_x][pixel_y])
        blues.append(blue)
        green = float(image.GetRasterBand(2).ReadAsArray()[pixel_x][pixel_y])
        greens.append(green)
        red = float(image.GetRasterBand(3).ReadAsArray()[pixel_x][pixel_y])
        reds.append(red)
        nir = float(image.GetRasterBand(4).ReadAsArray()[pixel_x][pixel_y])
        nirs.append(nir)
        ndvi = ((nir-red)/(red+nir)*1000)
        ndvis.append(ndvi)
        ndwi = ((green-nir)/(green+nir)*1000)
        ndwis.append(ndwi)
        qas.append(qa)
    return np.array([o_time,blues,greens,reds,nirs,ndvis,ndwis,qas])




    
    
        
        
