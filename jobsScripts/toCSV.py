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
import csv
import multiprocessing
from functools import partial
import json
from earthpy.spatial import normalized_diff as ndvi
import fnmatch


##########################################################
## resamples a single tif file and save result as tmp file

def resample_image_to_tmp(image_resolution, tif_file_name):
   
    #create tmp file name
    new_temp_file_name ='/tmp/' + str(image_resolution) + 'm_' + str(os.path.basename(tif_file_name))
    
    #check to see resampled images already exist and remove if so
    if os.path.isfile(new_temp_file_name):
        os.remove(new_temp_file_name)

    #resample images
    print('resampling {}'.format(os.path.basename(new_temp_file_name)))
    gdal.Warp(
            new_temp_file_name,
            tif_file_name, xRes=image_resolution,
            yRes=image_resolution,
            resampleAlg=gdal.GRA_Average
            )


def write_mapped_image_names_to_csv(output_csv_dir,sorted_image_stack_files):
    ######################################################
    # write image file name/date to csv file as row for referencing in later functions
    
    with open(
        output_csv_dir 
        + '/image_file_names' 
        + '.csv', 'a+') as images_mapped_csv:

        # create csv writer
        writer_object = writer(images_mapped_csv)

        for sorted_tif_image_name in sorted_image_stack_files:
            # append each image file name as new csv row
            writer_object.writerow([os.path.basename(sorted_tif_image_name)])

###############################################################
# resamples an entire image stack and saves result in tmp folder

def resample_image_stack_to_tmp(image_resolution,sorted_image_stack_files):
    
    # initiate multiprocessing with 4 cores
    p=multiprocessing.Pool(4)
    
    # if desired resolution is not PS native 3m, 
    # resamaple images and create list of filenames/path, 
    if image_resolution != 3:
       
        #create list of tmp resampled image names
        list_of_resampled_image_names =  [
        '/tmp/' + str(image_resolution) 
        + 'm_' + str(os.path.basename(tif_file_name)) 
        for tif_file_name in sorted_image_stack_files
        ]

        #resample images to tmp files using multiprocessing
        p.map(partial(resample_image_to_tmp, image_resolution), sorted_image_stack_files)
        
        #return resampled image file names + path  
        return list_of_resampled_image_names
    
    # if request images_resolution is 3m 
    # return original sorted image stack file names/path
    else:
        return sorted_image_stack_files

#########################################################
# write image metadata to json for use by later functions

def write_metadata_to_json(image_shape,geo,proj,image_resolution, num_rows_per_csv,output_csv_dir):
       
    # create dict of metadata
    image_metadata_dict = {
            "image_shape": image_shape,
            "geo": geo,
            "proj": proj,
            "image_resolution": image_resolution,
            "num_rows_per_csv": num_rows_per_csv
            }
    
    # write dict metadata to json file
    image_metadata_file = open(output_csv_dir + "/image_metadata.json", "w")
    image_metadata_file.write(json.dumps(image_metadata_dict))
    image_metadata_file .close()


##########################################################
# generate and return imagestack metadata

def get_image_stack_metadata(resampled_image_file_names ):

    # resample image to requested resoultion
    first_image_ds = gdal.Open(resampled_image_file_names[0])
    
    # output variables
    image_shape = np.shape(first_image_ds.ReadAsArray())
    proj=first_image_ds.GetProjection()
    geo=first_image_ds.GetGeoTransform()
    
    return image_shape,proj,geo

###########################################################
# determine number of csv files to create from num_rows_per_csv

def rows_to_csv_calc(image_shape, num_rows_per_csv):

    # variables
    _, _, cols = image_shape
    
    # calculate row #s
    return [
        (i,i + num_rows_per_csv)
        if i + num_rows_per_csv <= cols else (i,cols) 
        for i in range(0, cols, num_rows_per_csv)
        ]

####################################################################
## main function to loop through resampled images and append csv

def write_pixel_timeseries_data_to_csv_by_row(resampled_image_file_names,  output_csv_dir, image_resolution, zfill_len, pixel_rows_to_write):
        
        # variables
        start_row, end_row = pixel_rows_to_write
        num=0

       
        # loop through each reasampled image tif to extract pixel values and append a new csv for each image
        for resampled_image_file in resampled_image_file_names:
            print(
                'writing rows {} to {} for {} to CSV'.format(
                pixel_rows_to_write[0], 
                pixel_rows_to_write[1], 
                os.path.basename(resampled_image_file))
                )
            num+=1
            

            # open each file tif into dataset
            tif_dataset = gdal.Open(resampled_image_file)
            
            ######################################################
            # creates an array of pixel values for each band based on selected rows, 
            # flattens each band array into single list and appends list as new row to csv

            # create/open csv file for 4 PS bands
            for k in range(1,5):
                with open(
                    output_csv_dir + '/b' + str(k) 
                     + '_rows_' 
                    + str(start_row).zfill(zfill_len) 
                    + '_to_' + str(end_row).zfill(zfill_len) 
                    +  '_' + str(image_resolution)
                    + 'm.csv', 'a+'
                    ) as out_4band_csvs:

                    # create csv writer
                    writer_object = writer(out_4band_csvs)


                    # append band pixel-values, clipped to rows and flattened into one list, as new csv row
                    writer_object.writerow(np.array(tif_dataset.GetRasterBand(k).ReadAsArray()[start_row:end_row].flatten()))

            #############################################
            # calculate and append b5(ndvi) csv values by row
            
            # create/open csv ndvi file
            with open(
                    output_csv_dir + '/b5'
                     + '_rows_' 
                    + str(start_row).zfill(zfill_len) 
                    + '_to_' + str(end_row).zfill(zfill_len) 
                    +  '_' + str(image_resolution)
                    + 'm.csv', 'a+'
                    ) as out_ndvi_csv:

                    # create csv writer
                    writer_object = writer(out_ndvi_csv)

                    # calculate ndvi using earthpy.spatial normalized diff
                    # append ndvi to csv files by new row
                    writer_object.writerow((
                        ndvi(
                            tif_dataset.GetRasterBand(4).ReadAsArray()[start_row:end_row], 
                            tif_dataset.GetRasterBand(3).ReadAsArray()[start_row:end_row])
                             * 1000).flatten()
                             )
                
            
##########################################################################################
################################### Init Function ########################################
########################################################################################## 
# Takes input image_stack and writes data to csv files which can later be read and values
# used in CCD

def init(image_stack_dir, output_csv_dir, num_rows_per_csv, cores, image_resolution):
    
    
    # sort image files from directory
    sorted_image_stack_files = sorted(glob.glob(os.path.join(image_stack_dir, '*.tif')))

    # resample images to desired resolution and save temp files in new temp directory
    resampled_image_file_names = resample_image_stack_to_tmp(image_resolution, sorted_image_stack_files)
    
    # create output CSV folder
    
    if not os.path.isdir(output_csv_dir):
        os.mkdir(output_csv_dir)

    # if using init for first time, check for prexisiting files 
    # and delete them so it does not append pre-exisiting data
    for file in os.scandir(output_csv_dir):
        os.remove(file.path)


    # collect metadata
    image_shape, proj, geo = get_image_stack_metadata(resampled_image_file_names)
    
    # write metadata to json file
    write_metadata_to_json(
        image_shape,geo,proj, 
        image_resolution, 
        num_rows_per_csv, 
        output_csv_dir
        )

    # determine number of csv files to create using use input, num_rows_per_csv,
    #  and create tuples of row numbers for each multiprocessing job

    pixel_rows_to_write = rows_to_csv_calc(image_shape, num_rows_per_csv)
 
    
    # use multiprocessing to write rows to csv
    p = multiprocessing.Pool(cores)
    p.map(
        partial(
        write_pixel_timeseries_data_to_csv_by_row, 
        resampled_image_file_names, 
        output_csv_dir, 
        image_resolution, 
        len(str(image_shape[2]))), 
        pixel_rows_to_write
        )

    write_mapped_image_names_to_csv(output_csv_dir,sorted_image_stack_files)

 
##########################################################################################
################################### add_images function ########################################
########################################################################################## 
# function to scan image stack directory for new files and append pixel values to already existing csv files
# based on parameters set for previous images ingestion in original init function run

def add_images(image_stack_dir, output_csv_dir, cores):

    # create list of all images in image stack directory
    old_and_new_image_file_names = sorted(fnmatch.filter(os.listdir(image_stack_dir), "*.tif"))
    
    # create empty lists to append already mapped files 
    # and file which have not been mapped
    list_of_image_names_already_mapped = []
    list_of_image_names_not_mapped = []
    
    # open csv file containing files names 
    # of images already written to CSV 
    csv_of_mapped_file_names = open(
        glob.glob(
            os.path.join(
                output_csv_dir, '*image_file_names.csv'))[0]
                )
    csvreader = csv.reader(csv_of_mapped_file_names)

    # append files name of images already written to csv
    for row in csvreader:
        list_of_image_names_already_mapped.append(row[0])
    
    # check to see if images in image directory have already been mapped
    # if not, append new file name to list of files needing to be mapped
    for image_name in old_and_new_image_file_names:
        if list_of_image_names_already_mapped.count(image_name) == 0:
           list_of_image_names_not_mapped.append(os.path.join(image_stack_dir, image_name))
   
    # if there are new files, map them to CSV
    if len(list_of_image_names_not_mapped) > 0:
       
       # collect image metadata and original init settings needed to write 
       # new images to csv with same parametes
        json_dict = open(glob.glob(os.path.join(output_csv_dir, '*.json'))[0])
        image_metadata = json.load(json_dict)

        #calculate how many rows per csv, should match number numb of rows as the original init function
        pixel_rows_to_write = rows_to_csv_calc(image_metadata['image_shape'], image_metadata['num_rows_per_csv'])

        #resample the new images to the same resolution originally used in the init function
        new_resampled_image_file_names = resample_image_stack_to_tmp(image_metadata['image_resolution'], list_of_image_names_not_mapped)
        
        # use multiprocessing to append pixel data rows to previously created csv files for each band
        p = multiprocessing.Pool(cores)
        p.map(
            partial(
            write_pixel_timeseries_data_to_csv_by_row, 
            new_resampled_image_file_names, 
            output_csv_dir, 
            image_metadata['image_resolution'],
            len(str(image_metadata['image_shape'][2]))), 
            pixel_rows_to_write
            )

        #change variable name    
        newly_mapped_images_names = list_of_image_names_not_mapped

        # append recently mapped file names to csv of all mapped file names
        write_mapped_image_names_to_csv(output_csv_dir,newly_mapped_images_names )
    
    #if no new images to map in directory, print message
    else:
        print('No New Images To Add')
       

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
    cores = 4
    
    output_csv_dir = str(image_stack_dir) + '/pixel_values'
    
    #init(image_stack_dir,output_csv_dir, 100,4,30)

    add_images(image_stack_dir, output_csv_dir, cores)

if __name__ == '__main__':
    main()