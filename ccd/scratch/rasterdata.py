# import numpy as np
# import time
# import multiprocessing
# def print_values(arr):
#     rank = multiprocessing.current_process()
#     time.sleep(3)
#     print(rank, arr)
# def main():
#     size = 2
#     arr = np.array_split([0, 1, 2, 3, 4, 5], size)
#     print(arr) 
#     p = multiprocessing.Pool(size)
#     p.map(print_values, arr)
# if __name__ == '__main__':
#     main()



# import numpy as np
# import time
# import multiprocessing
# def print_values(arr):
#     rank = multiprocessing.current_process()
#     time.sleep(3)
#     print(rank, arr)
# def main():
#     size = 2
#     data = list(zip(range(6), 'a b c d e f'.split()))
#     arr = np.array_split(data, size)
#     print(arr) 
#     p = multiprocessing.Pool(size)
#     p.map(print_values, arr)
# if __name__ == '__main__':
#     main()

import cv2
import numpy as np
from osgeo import gdal

def transfer_16bit_to_8bit(image_path,band):
    image=gdal.Open(image_path)
    image_16bit=image.GetRasterBand(band).ReadAsArray()
    image_8bit=image.GetRasterBand(band).ReadAsArray().astype(np.uint8)
    print(image_16bit)
    min_16bit = np.min(image_16bit)
    max_16bit = np.max(image_16bit)
    min_8bit = np.min(image_8bit)
    max_8bit = np.max(image_8bit)
    image_8bit = np.array(np.rint((255 * (image_16bit - min_16bit)) / float(max_16bit - min_16bit)), dtype=np.uint8)
    print(min_8bit)
    print(max_8bit)
    print('16bit dynamic range: %d - %d' % (min_16bit, max_16bit))
    print('8bit dynamic range: %d - %d' % (np.min(image_8bit), np.max(image_8bit)))
    return image_8bit


def save_raster(path,arrayList,format='GTiff', dtype = gdal.GDT_Float32):
    rows,cols=np.shape(arrayList[0])
    image=gdal.Open(image_path) 
    arrays=arrayList
    tot_path=path+"8_bit.tif"
    # Initialize driver & create file
    driver = gdal.GetDriverByName(format)
    Image_out = driver.Create(tot_path,cols,rows,4,dtype)
    Image_out.SetGeoTransform(image.GetGeoTransform())
    Image_out.SetProjection(image.GetProjection())
    for k in range(len(arrayList)):
        Image_out.GetRasterBand(k+1).WriteArray(arrays[k])
    print("Rasters Saved")
    Image_out_out = None

image_path = '/Users/arthur.platel/desktop/Fusion_Images/CZU/PF-SR/2018-01-02.tif'
array1=transfer_16bit_to_8bit(image_path,1)
array2=transfer_16bit_to_8bit(image_path,2)
array3=transfer_16bit_to_8bit(image_path,3)
array4=transfer_16bit_to_8bit(image_path,4)
path="/Users/arthur.platel/Desktop/CCDC_Output/"
arrayList=[array1,array2,array3,array4]
save_raster(path,arrayList)