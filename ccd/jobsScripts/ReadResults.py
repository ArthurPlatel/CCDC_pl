import pandas as pd
import csv
import os
import glob


def main():
    csv_dir='/Users/arthur.platel/Desktop/Fusion_Images/CZU_FireV2/pixelValues_'
    folder='/Users/arthur.platel/Desktop/Fusion_Images/CZU_FireV2/pixelValues_'
    # d3ata=getImageData(folder)
    # shape,geo,proj=getImageData(folder)
    # print(shape)
    readResults(csv_dir)

def readResults(csv_dir):
    allFiles = glob.glob(os.path.join(csv_dir, '*.csv'))
    print(len(allFiles))
    for csvFile in allFiles:
        with open(csvFile) as csv:
            read=pd.read_csv(csv, delimiter=',',header=None,names=list(range(5))).dropna(axis='columns',how='all')
            for k in range
            print(eval(read[0][0]).count(737897))
    #         try:
    #              read[2]
    #              print('yesdfgadfgdgsdgdgdgfdgdgdfgsdfgdgsdgdgd')
    #         except KeyError:
    #             print('no')
    #         print(eval(read[1][0])['break_day'])
    #         print(eval(read[0][0])['pixel'])
    #         print(eval(read[0][0])['break_day'])
    #         # for k in range(len(read)):
    #         #     for l in range(0,2): 
    #         #         if pd.isnull(read[l][k])==False:
    #         #             print(eval(read[l][k])['pixel'])
    #         #             print(eval(read[l][k])['break_day'])

def getImageData(csv_dir):
    allFiles = glob.glob(os.path.join(csv_dir, '*.csv'))
    csvFile=allFiles[0]
    with open(csvFile) as csv:
        read=pd.read_csv(csv, delimiter=',',header=None,names=list(range(3))).dropna(axis='columns',how='all')
        shape=read[0][0]
        print(shape)
        geo=read[1][0]
        proj=read[2][0]
    #return shape, geo, proj

if __name__ == '__main__':
    main()