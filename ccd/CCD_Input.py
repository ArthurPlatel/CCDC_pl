import csv
import pandas as pd
import classification as cls
from datetime import date

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

csvFile='/Users/arthur.platel/Desktop/CCDC_Output/CZU_FireV2/CCD_resultsDict.csv'
clsf=cls.createClassifier()
classList=['barren','burned','redwood']
with open(csvFile) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    for row in csv_reader:
        df1=toDF(eval(row[0]))
        df2=toDF(eval(row[1]))
        class1=cls.classify(df1,clsf)
        class2=cls.classify(df2,clsf)
        # print(class1)
        # print(class2)
        # print(classList[int(class1)])
        print("pixel {} was {} until {} when it was {}".format(eval(row[0])["pixel"],classList[int(class1)],date.fromordinal(eval(row[0])["break_day"]),classList[int(class2)]))
        #print(len(row))#,#["pixel"])

