import datetime
import multiprocessing
import csv
from ccd.parameters import defaults as dfs
import os
import glob
from ccd import detect
import json
import argparse
import sys
from time import strptime
from functools import partial


def main():

    # ###Argparse settings
    parser = argparse.ArgumentParser(description="Calculate CCD results")
    parser.add_argument('path', type = str)
    parser.add_argument("--num_cores", type=int, help="number of cores to use in multiprocessing", default=4)
    parser.add_argument('--overwrite', action = 'store_true')
    args = parser.parse_args()
    ## add more ccd parameters to argparse later


    # check that the user supplied directory path exists, else print error message
    if not os.path.isdir(args.path):
        raise argparse.ArgumentTypeError(f"readable_dir:{args.path} is not a valid path")
    
    # create lists of directory files
    parent_csv_dir= str(args.path)+'/pixel_values'
    list_of_csv_dirs = sorted(glob.glob(os.path.join(parent_csv_dir, 'rows*')))

    #output results directory name based on date of ccd run
    results_dir_name = f'{parent_csv_dir}/CCD_results_{datetime.date.today()}'

    #if previous results exist, print error and reccomend overwrite
    if os.path.isdir(results_dir_name) and not args.overwrite:
        print(f'--{os.path.basename(results_dir_name)} directory already exists, running ReadResults.py again will overwrite previous results. Run with --overwrite to continue.')
        sys.exit(1)
    
    #if overwrite flag is used, delete preexisting results
    if os.path.isdir(results_dir_name) and args.overwrite:
        os.rmdir(results_dir_name)
    
    # create results directory based on date run
    if not os.path.isdir(results_dir_name):
        os.mkdir(results_dir_name)
    # check num of cores is positive
    if not ( args.num_cores > 0 ):
            raise argparse.ArgumentTypeError('Input Parameters --num_cores must be pssitive non-zero integers')
    
    # calculate ccd results for each pixel in for all csv directories
    for rows_dir in list_of_csv_dirs:
        single_csv_to_ccd_results(rows_dir, args.num_cores, results_dir_name)
####################################################
############## function ############################

def single_pixel_values_to_ccd_result(csv_pixel_files, dates, start_row, image_metadata,fname, k):
       
        # empty list to store band values for a given pixel
        col_values = []
        # read pixel values from CSV and format for CCD algorithim
        #########################################################

        # open all 5 csv files with values for each pixel 
        for csv_file in sorted(csv_pixel_files):
            with open(csv_file) as pixel_values:
                # read csv values
                csv_reader = csv.reader(pixel_values, delimiter=',')

                # extract b1,b2,b3,b4,b5 values for each pixel and append into col_values
                single_pixel_band_time_series = [float(row[k]) if row[k] !='--' else 0 for row in csv_reader ]
                col_values.append(single_pixel_band_time_series)

        # unpack values for ccd algorithim        
        blues, greens, reds, nirs, ndvis = col_values

        # create list of  quality values for fusion images in ccd algorithim, 
        # using qa value of 0 indicates that images are cloud free and can bypass ccd cloudmasking
        qas = [0]*len(blues)

        # run ccd algorithim on given pixel
        result = detect(dates, blues, greens, reds, nirs, ndvis, qas, dfs['params'])

        #calculate pixel coordinates
        pixel = ((k//image_metadata['image_shape'][2]) + start_row, k % image_metadata['image_shape'][2])
        print(f'processing pixel {pixel}')

        # create output dictionary with pixel coordinates as key
        pix_dict_for_json={str(pixel): result["change_models"]}

        # append output dictionary to json file
        with open(fname, "a+") as results_json :
            results_json.write(json.dumps(pix_dict_for_json))

################################################
################################################
# run ccd algoritim on each pixel with values stored in csv file, exports ccd algorithim outputs tojson file
def single_csv_to_ccd_results(rows_dir, cores, results_dir_name):
    
    # collect image_metadata
    csv_dir = os.path.dirname(rows_dir)
    print(csv_dir)
    json_dict = open(glob.glob(os.path.join(csv_dir, '*.json'))[0])
    image_metadata = json.load(json_dict)

    # store image_dates from csv file in list as ordinal values for ccd algorithim
    image_files_mapped = glob.glob(os.path.join(csv_dir,'*.csv'))[0]
    with open(image_files_mapped) as image_names:
        csv_reader = csv.reader(image_names)
        dates = [
             datetime.date(
             strptime(str(row[0]),'%Y-%m-%d.tif').tm_year,
             strptime(str(row[0]),'%Y-%m-%d.tif').tm_mon,
             strptime(str(row[0]),'%Y-%m-%d.tif').tm_mday
             ).toordinal() 
             for row in csv_reader
             ]
        
    # create list of csv files in folder to be processed, should be 5 files for every run
    csv_pixel_files = glob.glob(os.path.join(rows_dir,'*.csv'))
    

    # determine rows nums of mapped rows saved in csv files using the file name
    row_nums = []
    for string in os.path.basename(csv_pixel_files[0]).split("_"):
        if string.isdigit():
            row_nums.append(int(string))
    start_row, end_row = row_nums

    # calculate total number of pixels in csv files
    tot_pix = (end_row - start_row) * image_metadata['image_shape'][2]
    
    # vriable used in filename
    zfill_len = len(str(image_metadata['image_shape'][1]))

    #file name to save json ccd results to   
    fname = f'{results_dir_name}/CCD_rows_{start_row:0{zfill_len}d}_to_{end_row:0{zfill_len}d}_{image_metadata["image_resolution"]}m.json' 

    #still need to save ccd parameters to json file

    # check for previous file and remove
    if os.path.isfile(fname):
        os.remove(fname)   
    
    # create a list of values in range of exery pixel in csv file to be used by multiprocessing  
    to_ccd = list(range(tot_pix))

    # initiate multiprocessing with chosen number of cores
    p = multiprocessing.Pool(cores)

    # create ccd results for each pixel using multiprocessing
    p.map(partial(single_pixel_values_to_ccd_result, csv_pixel_files, dates, start_row, image_metadata, fname), to_ccd)
   
    
if __name__ == '__main__':
    main()

#     #do everything