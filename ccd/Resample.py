from ccd.CCD_master import resampleImage
import glob
from osgeo import gdal
from datetime import date
from datetime import time
import numpy as np
import os
from ccd.CCD_master import pixelCoordinates


def resampleImage_ordinal(images,ordinal,ras_data):
    y=date.fromordinal(ordinal).year
    m=date.fromordinal(ordinal).month
    d=date.fromordinal(ordinal).day
    resampleImage_date(images,y,m,d,ras_data)        
def resampleImage_date(images,year,month,day,ras_data):
    geo,proj,output,shape,y_size,sample_size=ras_data
    for image in images:
        days = date(int(image[-14:-10]), int(image[-9:-7]), int(image[-6:-4])).toordinal()
        date0=date(int(year),int(month),int(day)).toordinal()
        if date0==days:
            print(date.fromordinal(date0))
            image0=gdal.Open(image)
            image0=gdal.Warp(output+"/"+str(date.fromordinal(date0))+'_'+str(sample_size)+'m.tif',image0,xRes=sample_size,yRes=sample_size,resampleAlg='average')
            pixelCoordinates(ras_data)

parent_dir= '/Users/arthur.platel/Desktop/Fusion_Images/CZU_FireV2'
allFiles = glob.glob(os.path.join(parent_dir, '*.tif'))
#resample pixel size(m)
sample_size=30
#output_directory
output=parent_dir+str('/CCD_Output'+str(sample_size)+"m_resampled")
image0=gdal.Open(allFiles[0])
resampled=gdal.Warp('/vsimem/in_memory_output.tif',image0,xRes=sample_size,yRes=sample_size,resampleAlg=gdal.GRA_Average)
#Global get info for output files
geo=resampled.GetGeoTransform()
proj=resampled.GetProjection()

#global shape of image
shape=np.shape(resampled.ReadAsArray())
print(shape)
y_size=shape[2]

ras_data=geo,proj,output,shape,y_size,sample_size


# resampleImage_ordinal(allFiles,737820+7,ras_data)

# resampleImage_ordinal(allFiles,737820,ras_data)
# resampleImage_ordinal(allFiles,737820-7,ras_data)
resampleImage_date(allFiles,2021,7,1,ras_data)
