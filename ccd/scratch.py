
import numpy as np
import data_input as FF
from parameters import defaults as dfs
from datetime import date
from CCD_master import pixelCoordinates
from CCD_master import save_raster
from osgeo import gdal


#print(686-(686%5))

def fromOrdinal(number):
    print(date.fromordinal(number))


def pixelRaster(parent_dir,out_dir):
    images=FF.sortImages(parent_dir)
    image=gdal.Open(images[0])
    shape=FF.shape(images)
    geo=image.GetGeoTransform()
    proj=image.GetProjection()
    pixels=pixelCoordinates(shape)
    save_raster(1,[pixels],shape,"_pixelCoordinates.tif",out_dir,geo,proj)
    
# csvFile='/Users/arthur.platel/Desktop/CCDC_Output/CZU/PF-SR/CCD_resultsDict.csv'
# with open(csvFile) as csv_file:
#     csv_reader = csv.reader(csv_file, delimiter=',')
#     r=0
#     for row in csv_reader:
#         if r==1:
#             print(row[0])
#         r+=1

def main():
    # list=[1,2,3,4,5]
    # print(list[-1])
    # parent_dir='/Users/arthur.platel/Desktop/Fusion_Images/hospital/PF-SR'
    # out_dir="/Users/arthur.platel/Desktop/CCDC_Output/hospital"
    fromOrdinal(737846)
   

if __name__ == '__main__':
    main()
    

