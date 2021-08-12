from osgeo import gdal
import numpy as np
import glob

parent_dir='/Users/arthur.platel/Desktop/Fusion_Images/CZU_FireV2'

files = glob.glob(os.path.join(parent_dir, '*.tif'))
for fusiontif in files:
    image=gdal.Open(fusiontif)
    trans=gdal.Translate('/vsimem/in_memory_output.tif',image,xRes=30,yRes=30)
print(trans.GetGeoTransform())
war=gdal.Warp('/vsimem/in_memory_output.tif',image,xRes=30,yRes=30)
print(war.GetGeoTransform())
print(np.shape(war.GetRasterBand(1).ReadAsArray()))
