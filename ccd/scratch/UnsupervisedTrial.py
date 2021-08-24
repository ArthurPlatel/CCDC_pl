
from sklearn.cluster import KMeans
from osgeo import gdal
import numpy as np

naip_fn = '/Users/arthur.platel/Desktop/CCDC_Output/CZU_FireV2/CompleteWClassData/2021-06-28training.tif'
driverTiff = gdal.GetDriverByName('GTiff')
naip_ds = gdal.Open(naip_fn)
nbands = naip_ds.RasterCount
data = np.empty((naip_ds.RasterXSize*naip_ds.RasterYSize, nbands))
print(naip_ds.RasterXSize)
print(nbands)


bands=[1,2,3,4,5,6,9,12,13,14,15,17]
#for i in range(1, nbands+1):
for i in bands:
    print(i)
    band = naip_ds.GetRasterBand(i).ReadAsArray()
    data[:, i-1] = band.flatten()


km = KMeans(n_clusters=3)
km.fit(data)
km.predict(data)
out_dat = km.labels_.reshape((naip_ds.RasterYSize, naip_ds.RasterXSize))

clfds = driverTiff.Create('/Users/arthur.platel/Desktop/Trialclassified2.tif', naip_ds.RasterXSize, naip_ds.RasterYSize, 1, gdal.GDT_Float32)
clfds.SetGeoTransform(naip_ds.GetGeoTransform())
clfds.SetProjection(naip_ds.GetProjection())
clfds.GetRasterBand(1).SetNoDataValue(-9999.0)
clfds.GetRasterBand(1).WriteArray(out_dat)
clfds = None