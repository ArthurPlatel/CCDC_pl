import numpy as np
from osgeo import gdal
import glob, os
from datetime import date
import csv


pixel_x = 100
pixel_y = 100
csv_name = "SC_sub_qa"
parent_dir = '/Users/arthur.platel/YATSM/fusion_images/'

#initialize empty lists
# julian_time = []
# greens = []
# blues = []
# reds = []
# nirs = []
# ndvis = []


# with open(str(csv_name)+'.csv','w', newline=None) as csvp:
#     writer = csv.writer(csvp)
#     qa = 0
#     files = glob.glob(os.path.join(parent_dir, '*.tif'))
#     sorted = sorted(files)
#     print(sorted)
#     for fusion_tif in sorted:
#         image = gdal.Open(fusion_tif)
#         blue = float(image.GetRasterBand(1).ReadAsArray()[pixel_x][pixel_y])
#         #blues.append(blue)
#         green = float(image.GetRasterBand(2).ReadAsArray()[pixel_x][pixel_y])
#         #greens.append(green)
#         red = float(image.GetRasterBand(3).ReadAsArray()[pixel_x][pixel_y])
#         #reds.append(red)
#         nir = float(image.GetRasterBand(4).ReadAsArray()[pixel_x][pixel_y])
#         #nirs.append(nir)
#         ndvi = float((nir-red)/(red+nir))
#         #ndvis.append(ndvi)  
#         year = int(fusion_tif[-14:-10])
#         month = int(fusion_tif[-9:-7])
#         day = int(fusion_tif[-6:-4])
#         o_date = date(year, month, day)
#         #print(o_date)
#         gordinal = o_date.toordinal()
#         #julian_time.append(julian)
#         writer.writerow([gordinal, blue, green, red, nir, ndvi, qa])
    