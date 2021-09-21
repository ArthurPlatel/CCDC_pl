import os
import sys
import csv
import glob
import json
import fnmatch
import argparse
import numpy as np
from csv import writer
from osgeo import gdal
import multiprocessing
from functools import partial
from earthpy.spatial import normalized_diff as ndvi
import re 

##########################################################
##########################################################
##################### Functions ##########################
##########################################################
##########################################################


##########################################################
## resamples a single tif file and save result as tmp file

def resample_image_to_tmp(image_resolution, tif_file_name):
   
    #create tmp file name
    tif_file_basename = os.path.basename(tif_file_name)
    new_temp_file_name = f'/tmp/{image_resolution}m_{tif_file_basename}'
    
    #check to see resampled images already exist and remove if so
    if os.path.isfile(new_temp_file_name):
        os.remove(new_temp_file_name)

    #resample images
    print(f'resampling {tif_file_basename} to {image_resolution}m\n')
    gdal.Warp(
            new_temp_file_name,
            tif_file_name, xRes=image_resolution,
            yRes=image_resolution,
            resampleAlg=gdal.GRA_Average
            )


###############################################################
# resamples an entire image stack and saves result in tmp folder

def resample_image_stack_to_tmp(image_resolution, sorted_image_stack_files):
    
    # initiate multiprocessing with 4 cores
    p = multiprocessing.Pool(4)
    
    # if request images_resolution is 3m 
    # return original sorted image stack file names/path
    if image_resolution == 3:
        return sorted_image_stack_files

    # if desired resolution is not PS native 3m, 
    # resamaple images and create list of filenames/path, 
       
    #create list of tmp resampled image names
    list_of_resampled_image_names =  [
        f'/tmp/{image_resolution}m_{os.path.basename(fname)}' 
        for fname in sorted_image_stack_files
    ]

    #resample images to tmp files using multiprocessing
    p.map(partial(resample_image_to_tmp, image_resolution), sorted_image_stack_files)
    
    #return resampled image file names + path  
    return list_of_resampled_image_names


##########################################################
# generate and return imagestack metadata

def get_image_stack_metadata(resampled_image_file_names):

    # resample image to requested resoultion
    first_image_ds = gdal.Open(resampled_image_file_names[0])
    
    # output variables
    image_shape = np.shape(first_image_ds.ReadAsArray())
    proj = first_image_ds.GetProjection()
    geo = first_image_ds.GetGeoTransform()
    
    return image_shape, proj, geo


#########################################################
# write image metadata to json for use by later functions

def write_metadata_to_json(image_shape,geo,proj, image_resolution, num_rows_per_csv ,output_csv_dir):
       
    # create dict of metadata
    image_metadata_dict = {
            "image_shape": image_shape,
            "geo": geo,
            "proj": proj,
            "image_resolution": image_resolution,
            "num_rows_per_csv": num_rows_per_csv
            }
    
    # write dict metadata to json file
    with open(f"{output_csv_dir}/image_metadata.json", "w") as image_metadata_file:
        image_metadata_file.write(json.dumps(image_metadata_dict))


###############################################################
# determine number of csv files to create from num_rows_per_csv

def rows_to_csv_calc(image_shape, num_rows_per_csv):

    # variables
    _, rows, _ = image_shape
    
    # calculate row #s
    return [
        (i, i + num_rows_per_csv)
        if i + num_rows_per_csv <= rows else (i, rows) 
        for i in range(0, rows, num_rows_per_csv)
        ]

####################################################################
## main function to loop through resampled images and append values to csv
####################################################################

def write_pixel_timeseries_data_to_csv_by_row(resampled_image_file_names,  output_csv_dir, image_resolution, zfill_len, pixel_rows_to_write):
        

        # variables
        start_row, end_row = pixel_rows_to_write

        #create new directories for each row grouping
        output_csv_dir = output_csv_dir +'/rows_' + str(start_row) + '_to_' + str(end_row)
        
        if not os.path.isdir(output_csv_dir):
            os.mkdir(output_csv_dir)

        # loop through each reasampled image tif to extract pixel values and append a new csv for each image
        for resampled_image_file in resampled_image_file_names:
            
            # open each file tif into dataset
            tif_dataset = gdal.Open(resampled_image_file)
            
            ######################################################
            # creates an array of pixel values for each band based on selected rows, 
            # flattens each band array into single list and appends list as new row to csv

            # create/open csv file for 4 PS bands
            for k in range(1, 5):
                fname = f'{output_csv_dir}/b{k}_rows_{start_row:0{zfill_len}d}_to_{end_row:0{zfill_len}d}_{image_resolution}m.csv'
                with open(fname, 'a+') as out_4band_csvs:

                    # create csv writer
                    writer_object = writer(out_4band_csvs)

                    # append band pixel-values, clipped to rows and flattened into one list, as new csv row
                    writer_object.writerow(np.array(tif_dataset.GetRasterBand(k).ReadAsArray()[start_row:end_row].flatten()))

            #############################################
            # calculate and append b5(ndvi) csv values by row
            
            # create/open csv ndvi file
            fname = f'{output_csv_dir}/b5_rows_{start_row}_to_{end_row}_{image_resolution}m.csv'
            with open(fname, 'a+') as out_ndvi_csv:

                # create csv writer
                writer_object = writer(out_ndvi_csv)

                # calculate ndvi using earthpy.spatial normalized diff
                # append ndvi to csv files by new row
                writer_object.writerow((
                    ndvi(
                        tif_dataset.GetRasterBand(4).ReadAsArray()[start_row:end_row], 
                        tif_dataset.GetRasterBand(3).ReadAsArray()[start_row:end_row]
                        ) * 1000).flatten()
                    )
                

############################################################################
# for each image tif mapped to csv, record file name to csv for use by later functions

def write_mapped_image_names_to_csv(output_csv_dir, sorted_image_stack_files):
    
    # create/open csv file to append
    fname = f'{output_csv_dir}/image_file_names.csv'
    with open(fname, 'a+') as images_mapped_csv:

        # create csv writer
        writer_object = writer(images_mapped_csv)

        for sorted_tif_image_name in sorted_image_stack_files:
            
            # append each image file name as new csv row
            writer_object.writerow([os.path.basename(sorted_tif_image_name)])

   
##########################################################################################
################################### Init Function ########################################
########################################################################################## 
# Takes input image_stack and writes data to csv files which can later 
# be read and values used in CCD algorithim

def init(image_stack_dir, output_csv_dir, num_rows_per_csv, cores, image_resolution):
    
    # sort image files from directory
    sorted_image_stack_files = sorted(glob.glob(os.path.join(image_stack_dir, '*.tif')))
    print(f'\ningesting {len(sorted_image_stack_files)} files\n')

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
    zfill_len = len(str(image_shape[2]))
    
    # write metadata to json file
    write_metadata_to_json(
        image_shape, geo, proj, 
        image_resolution, 
        num_rows_per_csv, 
        output_csv_dir
        )

    # determine number of csv files to create using use input, num_rows_per_csv,
    #  and create tuples of row numbers for each multiprocessing job
    pixel_rows_to_write = rows_to_csv_calc(image_shape, num_rows_per_csv)
 
    # use multiprocessing to write rows to csv
    p = multiprocessing.Pool(cores)
    print('\nwriting rows to csv\n')
    p.map(
        partial(
        write_pixel_timeseries_data_to_csv_by_row, 
        resampled_image_file_names, 
        output_csv_dir, 
        image_resolution, 
        zfill_len), 
        pixel_rows_to_write
        )

    write_mapped_image_names_to_csv(output_csv_dir,sorted_image_stack_files)

    print("\ninit complete\n")

 
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
    print("checking for new images in directory\n")
    for image_name in old_and_new_image_file_names:
        if list_of_image_names_already_mapped.count(image_name) == 0:
           list_of_image_names_not_mapped.append(os.path.join(image_stack_dir, image_name))
   
    # if there are new files, map them to CSV
    if len(list_of_image_names_not_mapped) > 0:
        print(f"{len(list_of_image_names_not_mapped)} new images detected \nadding new images to CSV\n")
       
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
        print('\nwriting rows to csv\n')
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

        print(" new images successfully added")
    
    #if no new images to map in directory, print message
    else:
        print('No new images to add\n')
       

def main():
    # ############ Command Line Variables ################
    # ## Set arg parse commands
    parser = argparse.ArgumentParser(description="CCDC")
    parser.add_argument('path', type = str)
    parser.add_argument('--init', action = 'store_true')
    parser.add_argument('--add_images', action = 'store_true')
    parser.add_argument("--num_rows", type = int, help="number of pixel rows to save per CSV file", default=10)
    parser.add_argument("--image_resolution", type=int, help="image resolution to resample image stack", default=30)
    parser.add_argument("--num_cores", type=int, help="number of cores to use in multiprocessing", default=4)
    parser.add_argument('--overwrite', action = 'store_true')
    args = parser.parse_args()

    ##### argparse error filters and if user inputs #####
    ##### pass filter with no errors, run functions #####
    #####################################################
    #####################################################
    
    #check that the user supplied image directory path exists, else print error message
    if not os.path.isdir(args.path):
        raise argparse.ArgumentTypeError(f"readable_dir:{args.path} is not a valid path")

    #if directory exists, set variables
    image_stack_dir = args.path
    output_csv_dir = f'{image_stack_dir}/pixel_values'

    #check that only one function being run at a time
    if args.init and args.add_images:
        raise argparse.ArgumentTypeError("Cannot run -init and -add_images together, must run one or the other")

    #verify that one function has been selected
    if not args.init and not args.add_images:
        raise argparse.ArgumentTypeError('No Action Selected, must select one function to run (-init or -add_images')
    
    ##############################
    #if init function was selected
    if args.init:

        #check num_rows rows, num_cores and image_resoluton are positive numbers
        if not (args.num_rows > 0 and args.num_cores > 0 and args.image_resolution > 0):
            raise argparse.ArgumentTypeError('Input Parameters --num_rows, --num_cores and --image_resolution, must be pssitive non-zero integers')

        if os.path.isdir(output_csv_dir) and not args.overwrite:
            print('--pixel_values directory already exists, running init again will overwrite previous ingestions. Run with --overwrite to continue.')
            sys.exit(1)

        #if input variables are all valid, run init function
        init(image_stack_dir, output_csv_dir, args.num_rows, args.num_cores, args.image_resolution)
    
    #####################################
    # if add_images function was selected, print message
    if args.add_images:
        print('\nchecking that necessary files and directories exist\n')
        
        #check to make sure pixel_values directory exists
        if not os.path.isdir(output_csv_dir):
            print('pixel_value directory not found, must run init function on new image stack directory before running add_images')
            sys.exit(1)

        # check that image_metadata.json file exists
        if not os.path.isfile(f"{output_csv_dir}/image_metadata.json"):
            print("missing image_metadata.json file in pixel_values")
            sys.exit(1)

        json_dict = open(glob.glob(os.path.join(output_csv_dir, '*.json'))[0])
        image_metadata = json.load(json_dict)

        #check that image_file_names.csv exists
        if not os.path.isfile(f"{output_csv_dir}/image_file_names.csv"):
            print("missing image_file_names.csv ")
            sys.exit(1)

        #check that there are correct amount of csvs to write to in directory
        list_of_rows_to_csv = rows_to_csv_calc(image_metadata['image_shape'], image_metadata["num_rows_per_csv"])
    
        if len(list_of_rows_to_csv) != len(glob.glob(os.path.join(output_csv_dir, 'rows*'))):
            print('not enought row folders detected in pixel_values, rerun init function')
            sys.exit(1)

        for row_folder in glob.glob(os.path.join(output_csv_dir, 'rows*')):
            if len(os.listdir(row_folder)) != 5:
                    print('not enought csv files detected for writing, rerun init function')
                    sys.exit(1)

        #run add_images function
        print("all necessary files and directories exist\n")
        add_images(image_stack_dir, output_csv_dir, args.num_cores)


if __name__ == '__main__':
    main()