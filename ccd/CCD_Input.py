import csv
import pandas as pd

csvFile='/Users/arthur.platel/Desktop/CCDC_Output/ccdResults.csv'
with open(csvFile) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    r=0
    for row in csv_reader:
         if r==1:
             print(eval(row)['start_day'])
    #         print(eval(row[1])[0][0]["start_day"])
         r+=1

def toDF(seq):
    pixel=pd.DataFrame({
            #'RMSE blue':[seq["blue"]["rmse"]],
            #'RMSE green':[seq['green']['rmse']],
            'RMSE red':[seq['red']['rmse']],
            'RMSE nir':[seq['nir']['rmse']],
            'RMSE ndvi':[seq['ndvi']['rmse']],
            #'Coef1 blue':[seq['blue']["coefficients"][0]],
            #'Coef1 green':[seq['green']["coefficients"][0]],
            'Coef1 red':[seq['red']["coefficients"][0]],
            'Coef1 nir':[seq['nir']["coefficients"][0]],
            'Coef1 ndvi':[seq['ndvi']["coefficients"][0]],
        })
    return pixel