from genericpath import isdir
from osgeo.gdalconst import GA_ReadOnly
import argparse
import glob
import os
from osgeo import gdal
import numpy as np
import time
from datetime import date
from csv import writer
import multiprocessing
from functools import partial
import json



## will resample a single tif file and save result as tmp file
def resample_image_to_tmp(image_resolution, tif_file_name):
    new_temp_file_name ='/tmp/' + str(image_resolution) + 'm_' + str(os.path.basename(tif_file_name))
    if os.path.isfile(new_temp_file_name):
        os.remove(new_temp_file_name)
    print('resampling {}'.format(new_temp_file_name))
    gdal.Warp(
            new_temp_file_name,
            tif_file_name, xRes=image_resolution,
            yRes=image_resolution,
            resampleAlg=gdal.GRA_Average
            )

# resamples an entire image stack and saves result in tmp folder
def resample_image_stack_to_tmp(image_resolution,sorted_image_stack_files):
    
    #create list of temp resampled image names
    list_of_resampled_image_names =  [
        '/tmp/' + str(image_resolution) 
        + 'm_' + str(os.path.basename(tif_file_name)) 
        for tif_file_name in sorted_image_stack_files
        ]

    # resample images using multiprocessing with 4 cores
    p=multiprocessing.Pool(4)
    if image_resolution != 3:
        p.map(partial(resample_image_to_tmp, image_resolution), sorted_image_stack_files)
        
    #return resampled image file names + path  
    return list_of_resampled_image_names


def write_metadata_to_json(image_shape,geo,proj,image_resolution, num_rows_per_csv,init_output_csv_dir):
        #write to json file
    image_metadata_dict = {
            "image_shape": image_shape,
            "geo": geo,
            "proj": proj,
            "image_resolution": image_resolution,
            "num_rows_per_csv": num_rows_per_csv
            }
        #write metadata to json file
    image_metadata_file = open(init_output_csv_dir + "/image_metadata.json", "w")
    image_metadata_file.write(json.dumps(image_metadata_dict))
    image_metadata_file .close()


def get_image_stack_metadata(resampled_image_file_names ):

    #resample image to requested resoultion
    first_image_ds = gdal.Open(resampled_image_file_names[0])
    
    #output variables
    image_shape = np.shape(first_image_ds.ReadAsArray())
    proj=first_image_ds.GetProjection()
    geo=first_image_ds.GetGeoTransform()
    
    return image_shape,proj,geo

#determine number of csv files to create from num_rows_per_csv
def rows_to_csv_calc(image_shape, num_rows_per_csv):
    _, _, cols = image_shape
    return [
        (i,i + num_rows_per_csv)
        if i + num_rows_per_csv <= cols else (i,cols) 
        for i in range(0, cols, num_rows_per_csv)
        ]

def write_pixel_timeseries_data_to_csv_by_row(resampled_image_file_names,  init_output_csv_dir, image_resolution, pixel_rows_to_write):
        start_row, end_row = pixel_rows_to_write
        num=0
        for resampled_image_file_name in resampled_image_file_names:
            print('writing image {} data to CSV'.format(num))
            num+=1
            for k in range(1,5):
                with open(
                    init_output_csv_dir + '/b' + str(k) 
                    + '_' + str(image_resolution) + 'm_rows_' 
                    + str(start_row).zfill(4) + '_to_' + str(end_row).zfill(4) 
                    + '.csv', 'a+'
                    ) as out_csv:

                    tif_dataset = gdal.Open(resampled_image_file_name)
                    band_dataset = np.array(tif_dataset.GetRasterBand(k).ReadAsArray()[start_row:end_row].flatten())
                    print(len(band_dataset))
                    writer_object = writer(out_csv)
                    writer_object.writerow(band_dataset)
                
                
    
        




def init(image_stack_dir, init_output_csv_dir, num_rows_per_csv, cores, image_resolution):
    
    
    #load and sort files from directory
    image_stack_files = glob.glob(os.path.join(image_stack_dir, '*.tif'))
    sorted_image_stack_files = sorted(image_stack_files)

    #resample images to desired resolution and save temp files in new temp directory
    resampled_image_file_names = resample_image_stack_to_tmp(image_resolution, sorted_image_stack_files)
    
    #create output CSV folder
    
    if not os.path.isdir(init_output_csv_dir):
        os.mkdir(init_output_csv_dir)

    #collect metadata
    image_shape, proj, geo = get_image_stack_metadata(resampled_image_file_names)
    
    #write metadata to json file
    write_metadata_to_json(image_shape,geo,proj,image_resolution, num_rows_per_csv,init_output_csv_dir)

    #determine number of csv files to create from num_rows_per_csv
    pixel_rows_to_write = rows_to_csv_calc(image_shape, num_rows_per_csv)

    write_pixel_timeseries_data_to_csv_by_row(resampled_image_file_names,  init_output_csv_dir, image_resolution, pixel_rows_to_write[2])

    # read_and_write_images_to_csv()
    # ReadImage
    

    
    # image = gdal.Open('/tmp/CCD_resampled_tif_files/30m_2017-07-11.tif')
    # blue_array=np.array(image.GetRasterBand(1).ReadAsArray())
    # print(blue_array[0][0])
    # print(np.array('/tmp/' + str(image_resolution) + 
    #         'm_' + str(os.path.basename(sorted_image_stack_files[0])).GetRasterBand(1).ReadAsArray().flatten()))#     #get fusion tile parameters
#     shape,proj,geo = get_image_metadata(sorted_image_stack_files,resolution)
    
#     #divide total pixels into smaller subets 
#     pixels=pixelsPool(shape,)
    
#     # initiate multiprocessing
#     p = multiprocessing.Pool(cores)

#     #create blank csv files to be written later
#     p.map(partial(create_CSV,shape,sample_size,output),pixels)

#     # write and append all pixel data to CSV files
#     p.map(partial(construct,sortedFiles,shape,sample_size,output,proj,geo),pixels)
    

def main():
    # ############ Command Line Variables ################
    # ## Set Parser flags
    # parser = argparse.ArgumentParser(description="CCDC", allow_abbrev=False, add_help=False)
    # parser.add_argument("-f",action="store", metavar="action", type=str, help="choose function ( i= init, a = addImages)")
    # parser.add_argument("-d",action="store", metavar="directory", type=str, help="Directory of image stack")
    # parser.add_argument("--r", action="store", metavar="value", type=int, help="Output resolution to resample image stack", default=30)
    # parser.add_argument("--p", action="store", metavar="value", type=int, help="Number of pixels to process per CSV file", default=400*400)
    # parser.add_argument("--c", action="store", metavar="value", type=int, help="Number of cores to use in multiprocessing", default=4)
    # parser.add_argument("-h", "--help", action="help", help="Display this message")
    # args = parser.parse_args()
    # ## Get Variables
    # parent_dir=args.d
    # sample_size=args.r
    # pixel_count=args.p
    # cores=args.c
    image_stack_dir='/Users/arthur.platel/Desktop/PF-SR'
    
    init_output_csv_dir = str(image_stack_dir) + '/pixel_values'
    
    init(image_stack_dir,init_output_csv_dir, 100,4,30)



if __name__ == '__main__':
    main()