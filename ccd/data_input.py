##############################################
##This Script is only used in notebooks#######
##############################################



import numpy as np
from osgeo import gdal
import glob, os
from datetime import date
import time
from ccd import detect
from ccd.parameters import defaults as dfs





def get_data(parent_dir,pixel_x,pixel_y,sampleSize,nth,d='fusion'):
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
    final= sortImages(parent_dir,nth)
    print("images used:",len(final))
    for fusion_tif in final:
        image0=gdal.Open(fusion_tif)
        if sampleSize!=3 and d=='fusion':
            image=gdal.Warp('/vsimem/in_memory_output.tif',image0,xRes=sampleSize,yRes=sampleSize,resampleAlg=gdal.GRA_Average)
        else:
            image=image0
        if d=='fusion':
            gordinal = date(int(fusion_tif[-14:-10]), int(fusion_tif[-9:-7]), int(fusion_tif[-6:-4])).toordinal()
        else:
            try:
                gordinal = date(int(fusion_tif[-39:-35]), int(fusion_tif[-35:-33]), int(fusion_tif[-33:-31])).toordinal()
            except ValueError:
                gordinal = date(int(fusion_tif[-37:-33]), int(fusion_tif[-33:-31]), int(fusion_tif[-31:-29])).toordinal()
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
        qas.append(qa)
    return np.array([o_time,blues,greens,reds,nirs,ndvis,qas])



def sortImages(parent_dir,nth):
    files = glob.glob(os.path.join(parent_dir, '*.tif'))
    file_list=[]
    for k in range(len(files)):
        if k%nth==0:
            file_list.append(files[k])
        images = sorted(file_list)
    return images

def shape(sortedImages):
    image=gdal.Open(sortedImages[0])
    image=gdal.Warp('/vsimem/in_memory_output.tif',image,xRes=dfs['resampleSize'],yRes=dfs['resampleSize'],sampleAlg='average')
    shape=np.shape(image.ReadAsArray())
    return shape




    
    
        
        
