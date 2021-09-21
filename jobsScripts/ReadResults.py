import datetime
import posix
import pandas as pd
import csv
from ccd.parameters import defaults as dfs
import os
import glob
from ccd import detect
import json
import argparse
import time
import toCSV
from time import strptime


def main():

    csv_dir= '/Users/arthur.platel/Desktop/Fusion_Images/CZU_FireV2/pixel_values' #'/Users/arthur.platel/Desktop/PF-SR/pixel_values'
    
    # ###Argparse settings
    # parser = argparse.ArgumentParser(description="CCDC", allow_abbrev=False, add_help=False)
    # parser.add_argument("-d",action="store", metavar="directory", type=str, help="Directory of image stack")
    # args = parser.parse_args()
    
    #variables
    #parent_dir=args.d
    #csv_dir= str(parent_dir)+'/pixel_values'
    row_dir = sorted(glob.glob(os.path.join(csv_dir, 'rows*')))
    #imageData=glob.glob(os.path.join(csv_dir, 'imageData*.csv'))[0]
    
    #-****run code on each data CSV file in directory/can change to single file
    #for file in files:
    readResults(row_dir[2],csv_dir)

def readResults(row_dir, csv_dir):
    
    # collect image_metadata
    json_dict = open(glob.glob(os.path.join(csv_dir, '*.json'))[0])
    image_metadata = json.load(json_dict)

    # collect image_dates from csv file
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
        
    #calculate how many rows per csv, should match number numb of rows as the original init function
    pixel_rows_to_write = toCSV.rows_to_csv_calc(image_metadata['image_shape'], image_metadata['num_rows_per_csv'])
    
    csv_pixel_values = glob.glob(os.path.join(row_dir,'*.csv'))
    row_nums=[]

    # determine which rows were mapped from file name
    for string in os.path.basename(csv_pixel_values[0]).split("_"):
        if string.isdigit():
            row_nums.append(int(string))
    start_row, end_row = row_nums
    pix_values = []
    tot_pix = (end_row - start_row) * image_metadata['image_shape'][2]
    zfill_len = len(str(image_metadata['image_shape'][1]))
    fname = f'{csv_dir}/CCDresults_rows_{start_row:0{zfill_len}d}_to_{end_row:0{zfill_len}d}_{image_metadata["image_resolution"]}m.json'    
    with open(fname, "a+") as results_json :
        for k in range(tot_pix):
            x = (k//image_metadata['image_shape'][2]) + start_row
            y = k % image_metadata['image_shape'][2]
            row_values=[]
            print(f'processing pixel {(x,y)}')
            for csv_file in sorted(csv_pixel_values):
                with open(csv_file) as pixel_values:
                    csv_reader = csv.reader(pixel_values, delimiter=',')
                    single_pixel_band_time_series = [float(row[k]) for row in csv_reader]
                    print(single_pixel_band_time_series)
                    row_values.append(single_pixel_band_time_series)
            blues, greens, reds, nirs, ndvis = row_values
            qas = [0]*len(blues)
            result = detect(dates, blues, greens, reds, nirs, ndvis, qas, dfs['params'])
            pix_dict_for_json={str((x,y)): result["change_models"]}
            results_json.write(json.dumps(pix_dict_for_json))
        
        
        
   

    # #pandas
    # start_df = time.time()
    # df = pd.read_csv(file, header=None)
    # pixel_values= list(df[5])
    # end_df = time.time() - start_df
    # print(pixel_values, end_df)


    # # not pandas
    # start = time.time()
    # with open(file) as csv_file:
    #     csv_reader = csv.reader(csv_file, delimiter=',')
    #     pix_5 = [int(row [5]) for row in csv_reader]
       
    # end = time.time() - start
    # print(pix_5, end)
        #     content = list(row[0])
        # print(content)


    #read csv file and imageData CSV
    
#     ####Variables######
#     pixel_count=int(df2[1][0])
#     shape=eval(df2[2][0])
#     dates =eval(df2[4][0])
#     params=dfs['params']
#     qas=[1]*len(dates)
#     pixel1=eval(df['pixel'][0])
#     num=(((pixel1[0]*shape[1])+pixel1[1]))//pixel_count
    
#     #determine CSV number to write
#     with open(str(csv_dir)+'/'+str(num)+'_Results.json', 'w') as outfile:
#             json.dump([],outfile)
    
#     ####Calculate CCD for each pixel
#     print("calulating CCD Results")
#     for pix in range(pixel_count):
#         start=time.time()
#         pixel=eval(df['pixel'][pix])
#         data =[[eval(df[band][pix])]for band in df.keys()[2:7]]
#         blues,greens,reds,nirs,ndvis=data
#         results=detect(dates,blues[0],greens[0],reds[0],nirs[0],ndvis[0],qas,params)
#         ##write results to json
#         ccdResults={str(pixel):results}
#         with open(str(csv_dir)+'/'+str(num)+'_Results.json', 'r') as outfile:
#             dat=json.load(outfile) 
#             dat.append(ccdResults)
#         with open(str(csv_dir)+'/'+str(num)+'_Results.json', "w") as file:
#             json.dump(dat, file)
#         end=time.time()-start
#         print('processed pixel {} in {}'.format(pixel,end))


if __name__ == '__main__':
    main()

#     #do everything