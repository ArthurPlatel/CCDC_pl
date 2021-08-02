from osgeo import gdal
import os
import sys
from ccd.data_input import get_data
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.insert(0, module_path)
import matplotlib.pyplot as plt
from datetime import datetime
import numpy as np
import fnmatch
import glob
import ccd


#Paramaters to eventually remove once code is streamlined for fusion 
# temporarily here to get code running
params = {'QA_BITPACKED': False,
              'QA_FILL': 255,
              'QA_CLEAR': 0,
              'QA_WATER': 1,
              'QA_SHADOW': 2,
              'QA_SNOW': 3,
              'QA_CLOUD': 4}
pixel_x = 100
pixel_y = 100
image_dir = '/Users/arthur.platel/CZU_fire' #YATSM/fusion_images/'
bands = ["blue","green","red","nir","ndvi"]


def plot(band,bands,results,dates):
    predicted_values = []
    prediction_dates = []
    break_dates = []
    start_dates = []
    mask = np.array(results['processing_mask'], dtype=bool)

    for num, result in enumerate(results['change_models']):
        days = np.arange(result['start_day'], result['end_day'] + 1)
        prediction_dates.append(days)
        break_dates.append(result['break_day'])
        start_dates.append(result['start_day'])
        
        intercept = result[band]['intercept']
        coef = result[band]['coefficients']
        
        predicted_values.append(intercept + coef[0] * days +
                                coef[1]*np.cos(days*1*2*np.pi/365.25) + coef[2]*np.sin(days*1*2*np.pi/365.25) +
                                coef[3]*np.cos(days*2*2*np.pi/365.25) + coef[4]*np.sin(days*2*2*np.pi/365.25) +
                                coef[5]*np.cos(days*3*2*np.pi/365.25) + coef[6]*np.sin(days*3*2*np.pi/365.25))
        
    plt.style.use('ggplot')
    fg = plt.figure(figsize=(16,9), dpi=300)
    if band=='ndvi':
        a1 = fg.add_subplot(2, 1, 1, xlim=(min(dates), max(dates)))     
    elif band=='nir':
        a1 = fg.add_subplot(2, 1, 1, xlim=(min(dates), max(dates)), ylim=(0, 5000))
    else:
        a1 = fg.add_subplot(2, 1, 1, xlim=(min(dates), max(dates)), ylim=(0, 600))

    for _preddate, _predvalue in zip(prediction_dates, predicted_values):
        a1.plot(_preddate, _predvalue, 'orange', linewidth=1)

    # fg = plt.figure(figsize=(16,9), dpi=300)
    # a1 = fg.add_subplot(2, 1, 1, xlim=(min(dates), max(dates), ylim=(-5000, 5000))

    # Predicted curves
    for _preddate, _predvalue in zip(prediction_dates, predicted_values):
        a1.plot(_preddate, _predvalue, 'orange', linewidth=2)

    a1.plot(dates[mask], bands[mask], 'g+') # Observed values
    a1.plot(dates[~mask], bands[~mask], 'k+') # Observed values masked out
    for b in break_dates: a1.axvline(b)
    for s in start_dates: a1.axvline(s, color='r')
    plt.title(band)
    return a1


# def fusion_pixel(pixel_x,pixel_y, image_dir,bands, plots=False):
#     #import data from images
#     data = get_data(image_dir,pixel_x,pixel_y)
#     dates, blues, greens, reds, nirs, ndvis, qas = data
#     results = ccd.detect(dates, blues, greens, reds, nirs, ndvis, qas, params=params)
#     if plots == True:
#         for k in range(len(bands)):
#             band_plot = plot(str(bands[k]),data[k+1],results,dates)
#             band_plot.imshow()
#     return results

def pixel_array(pixel_x,pixel_y, dict):
    pixel=dict[pixel_x,pixel_y]
    return np.array([pixel['dates'], pixel['blue'], pixel['green'], pixel['red'], pixel['nir'], pixel['ndvi'],pixel['qas']])


def fusion_pixel(pixel_x,pixel_y, dict):
    data = pixel_array(pixel_x,pixel_y,dict)
    dates, blues, greens, reds, nirs, ndvis, qas = data
    results = ccd.detect(dates, blues, greens, reds, nirs, ndvis, qas, params=params)
    if plots == True:
        for k in range(len(bands)):
            band_plot = plot(str(bands[k]),data[k+1],results,dates)
            band_plot.imshow()
    return results


import time
# start_time = time.time()
# fusion_pixel(pixel_x,pixel_y,image_dir)
# print("--- %s seconds ---" % (time.time() - start_time))

def create_raster(day,band, type,dict,shape):
    band_array=np.empty(shape)
    for x in range(shape[0]):
        for y in range(shape[1]):
            start_time = time.time()
            for num, result in enumerate(dict[x,y]):
                print("start date:{} \n end: date: {}".format(result[str(band)[type]]))
            pixel_time=time.time()-start_time
            print("sorted diction RMSE in :{}".format(pixel_time))
            # output_dict[x,y]=value["change_models"]
            # n+=1
            # pixel_time=time.time()-start_time
            # print("Completed Pixel: {},{} \nCompleted in: {} \nTotal Pixels Completed: {} of {} \nTotal Time: {}hrs.\n------------------------\n".format(x,y,pixel_time,n,total,total_time/3600))
            # total_time+=pixel_time



def fusion_image(image_dir,bands):
    files = glob.glob(os.path.join(image_dir, '*.tif'))
    fileA = files[0]
    image = gdal.Open(fileA)
    image_array = (image.GetRasterBand(1)).ReadAsArray()
    shape = image_array.shape
    print("shape:{}".format(shape))
    output = np.empty((2,2)) #shape)
    total =(shape[0])*(shape[1])
    n=0
    total_time=0
    output_dict={}
    for x in range(2):
        for y in range(2):
            start_time = time.time()
            value = fusion_pixel(x,y,image_dir,bands)
            output_dict[x,y]=value["change_models"]
            n+=1
            pixel_time=time.time()-start_time
            print("Completed Pixel: {},{} \nCompleted in: {} \nTotal Pixels Completed: {} of {} \nTotal Time: {}hrs.\n------------------------\n".format(x,y,pixel_time,n,total,total_time/3600))
            total_time+=pixel_time
    # type = "rmse"
    # create_raster(736700,"blue","rmse", output_dict, (4,4))
    return output_dict

final=fusion_image(image_dir,bands)
print(final)



