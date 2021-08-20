#Imports for classification
from osgeo import gdal
import numpy as np
import geopandas as gpd
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.metrics import confusion_matrix
from sklearn.metrics import plot_confusion_matrix
from ccd import parameters
import matplotlib.pyplot as plt
from ccd.CCD_Pool import save_raster
import time



#################################
##########Classification#########
# Function to create classifier
def rasterData(image):
    geo=image.GetGeoTransform()
    proj=image.GetProjection()
    #image1=image.ReadAsArray()[0:20,0:5,0:9]
    shape=np.shape(image.ReadAsArray())
    band,row,col=shape
    rd=pd.DataFrame({
        'RMSE blue':[(image.GetRasterBand(1).ReadAsArray())[x][y]for y in range(col) for x in range(row)],
        'RMSE green':[(image.GetRasterBand(2).ReadAsArray())[x][y]for y in range(col) for x in range(row)],
        'RMSE red':[(image.GetRasterBand(3).ReadAsArray())[x][y]for y in range(col) for x in range(row)],
        'RMSE nir':[(image.GetRasterBand(4).ReadAsArray())[x][y]for y in range(col) for x in range(row)],
        'RMSE ndvi':[(image.GetRasterBand(5).ReadAsArray())[x][y]for y in range(col) for x in range(row)],
        'Coef1 blue':[(image.GetRasterBand(7).ReadAsArray())[x][y]for y in range(col) for x in range(row)],
        'Coef2 blue':[(image.GetRasterBand(8).ReadAsArray())[x][y]for y in range(col) for x in range(row)],
        'Coef3 blue':[(image.GetRasterBand(9).ReadAsArray())[x][y]for y in range(col) for x in range(row)],
        'Coef1 green':[(image.GetRasterBand(10).ReadAsArray())[x][y]for y in range(col) for x in range(row)],
        'Coef2 green':[(image.GetRasterBand(11).ReadAsArray())[x][y]for y in range(col) for x in range(row)],
        'Coef3 green':[(image.GetRasterBand(12).ReadAsArray())[x][y]for y in range(col) for x in range(row)],
        'Coef1 red':[(image.GetRasterBand(13).ReadAsArray())[x][y]for y in range(col) for x in range(row)],
        'Coef2 red':[(image.GetRasterBand(14).ReadAsArray())[x][y]for y in range(col) for x in range(row)],
        'Coef3 red':[(image.GetRasterBand(14).ReadAsArray())[x][y]for y in range(col) for x in range(row)],
        'Coef1 nir':[(image.GetRasterBand(16).ReadAsArray())[x][y]for y in range(col) for x in range(row)],
        'Coef2 nir':[(image.GetRasterBand(17).ReadAsArray())[x][y]for y in range(col) for x in range(row)],
        'Coef3 nir':[(image.GetRasterBand(18).ReadAsArray())[x][y]for y in range(col) for x in range(row)],
        'Coef1 ndvi':[(image.GetRasterBand(19).ReadAsArray())[x][y]for y in range(col) for x in range(row)],
        'Coef2 ndvi':[(image.GetRasterBand(20).ReadAsArray())[x][y]for y in range(col) for x in range(row)],
        'Coef3 ndvi':[(image.GetRasterBand(21).ReadAsArray())[x][y]for y in range(col) for x in range(row)],
    })
    return rd


def createClassifier():
    gdf = gpd.read_file(parameters.defaults['trainingData'])
    classes = gdf["label"].unique()
    class_names=[0 for k in range(len(classes)) ]
    class_ids=gdf['id'].unique()
    print(class_ids)
    print(classes)
    for k in range(len(classes)):
        class_names[int(class_ids[k])]=classes[k]
    print("\nBuilding Random Forest Classifier")
    print("LandClasses: {} With values {}\n".format(classes,class_ids))
    print(len(gdf))
    data=pd.DataFrame({
        'RMSE blue':[gdf.iloc[k][2]for k in range(len(gdf))],
        'RMSE green':[gdf.iloc[k][3]for k in range(len(gdf))],
        'RMSE red':[gdf.iloc[k][4]for k in range(len(gdf))],
        'RMSE nir':[gdf.iloc[k][5]for k in range(len(gdf))],
        'RMSE ndvi':[gdf.iloc[k][6]for k in range(len(gdf))],
        'Coef1 blue':[gdf.iloc[k][7]for k in range(len(gdf))],
        'Coef2 blue':[gdf.iloc[k][8]for k in range(len(gdf))],
        'Coef3 blue':[gdf.iloc[k][9]for k in range(len(gdf))],
        'Coef1 green':[gdf.iloc[k][10]for k in range(len(gdf))],
        'Coef2 green':[gdf.iloc[k][11]for k in range(len(gdf))],
        'Coef3 green':[gdf.iloc[k][12]for k in range(len(gdf))],
        'Coef1 red':[gdf.iloc[k][13]for k in range(len(gdf))],
        'Coef2 red':[gdf.iloc[k][14]for k in range(len(gdf))],
        'Coef3 red':[gdf.iloc[k][15]for k in range(len(gdf))],
        'Coef1 nir':[gdf.iloc[k][16]for k in range(len(gdf))],
        'Coef2 nir':[gdf.iloc[k][17]for k in range(len(gdf))],
        'Coef3 nir':[gdf.iloc[k][18]for k in range(len(gdf))],
        'Coef1 ndvi':[gdf.iloc[k][19]for k in range(len(gdf))],
        'Coef2 ndvi':[gdf.iloc[k][20]for k in range(len(gdf))],
        'Coef3 ndvi':[gdf.iloc[k][21]for k in range(len(gdf))],
        'landcover':[gdf.iloc[k][0]for k in range(len(gdf))]
    })

    X=data[['RMSE blue','RMSE green','RMSE red','RMSE nir','RMSE ndvi','Coef1 blue','Coef2 blue','Coef3 blue','Coef1 green','Coef2 green','Coef3 green','Coef1 red','Coef2 red','Coef3 red','Coef1 nir',"Coef2 nir",'Coef3 nir','Coef1 ndvi','Coef2 ndvi','Coef3 ndvi']]  #'Coef1 blue','Coef1 green','RMSE blue','RMSE green' Features
    y=data['landcover']  # Labels

    # Split dataset into training set and test set
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3) # 70% training and 30% test

    #Create a Gaussian Classifier
    clf=RandomForestClassifier(parameters.defaults["n_estimators"],oob_score=parameters.defaults['oob_score'])

    #Train the model using the training sets y_pred=clf.predict(X_test)
    clf.fit(X_train,y_train)
    y_pred=clf.predict(X_test)
    accuracy = accuracy_score(y_test,y_pred)
    confusion= confusion_matrix(y_test,y_pred)
    # Plot non-normalized confusion matrix
    # titles_options = [("Confusion matrix, without normalization", None),
    #               ("Normalized confusion matrix", 'true')]
    # for title, normalize in titles_options:
    #     disp = plot_confusion_matrix(clf, X_test, y_test,
    #                                 display_labels=class_names,
    #                                 cmap=plt.cm.Blues,
    #                                 normalize=normalize)
    #     disp.ax_.set_title(title)
    #     print(title)
    #     print(disp.confusion_matrix)
    # plt.show()
    #Print accuracy results
    if parameters.defaults['oob_score']:
        #print(confusion)
        print(f'Out-of-bag score estimate: {clf.oob_score_:.3}')
        print(f'Mean accuracy score: {accuracy:.3}')
        print(f'Weight importance:{clf.feature_importances_}')
    print("Classifier Complete \n")
    return clf



    # b='/Users/arthur.platel/Desktop/ResampleTest/2018-01-31training.tif'
    # a='/Users/arthur.platel/Desktop/ResampleTest/2021-06-28training.tif'
    # before=gdal.Open(b)
    # after=gdal.Open(a)
    # geo=before.GetGeoTransform()
    # proj=before.GetProjection()
    # #shape=np.shape(before)
    
    # # # plt.imshow(crop[0])
    # # # plt.show()
    # print("Classifying Rasters")
    # dfBefore=rasterData(before)
    # dfAfter=rasterData(after)
    # predictBefore=clf.predict(dfBefore)
    # predictAfter=clf.predict(dfAfter)
    # shape=np.shape(before.ReadAsArray())
    # outputArrays=[np.zeros((shape[1],shape[2])),np.zeros((shape[1],shape[2]))]
    # for k in range(len(predictBefore)):
    #     r=int(k/(shape[2]))
    #     c=k%(shape[2])
    #     outputArrays[0][r][c]=predictBefore[k]
    # for k in range(len(predictAfter)):
    #     r=int(k/(shape[2]))
    #     c=k%(shape[2])
      
    #     outputArrays[1][r][c]=predictBefore[k]
    # output='/Users/arthur.platel/Desktop/'
    # save_raster(1,outputArrays,shape,"fireRasterTrial",output,geo,proj)


    


#         #Classification function

# #clf=createClassifier()
def classify(df,classifier):
    class_pred=classifier.predict(df)[0]
    return class_pred

# #helper function to load CCD result into readable dataframe for classification
# def toDF(seq):
#     pixel=pd.DataFrame({
#             'RMSE blue':[seq["blue"]["rmse"]],
#             'RMSE green':[seq['green']['rmse']],
#             'RMSE red':[seq['red']['rmse']],
#             'RMSE nir':[seq['nir']['rmse']],
#             'RMSE ndvi':[seq['ndvi']['rmse']],
#             'Coef1 blue':[seq['blue']["coefficients"][0]],
#             'Coef2 blue':[seq['blue']["coefficients"][1]],
#             'Coef3 blue':[seq['blue']["coefficients"][2]],
#             'Coef1 green':[seq['green']["coefficients"][0]],
#             'Coef2 green':[seq['green']["coefficients"][1]],
#             'Coef3 green':[seq['green']["coefficients"][2]],
#             'Coef1 red':[seq['red']["coefficients"][0]],
#             'Coef2 red':[seq['red']["coefficients"][1]],
#             'Coef3 red':[seq['red']["coefficients"][2]],
#             'Coef1 nir':[seq['nir']["coefficients"][0]],
#             'Coef2 nir':[seq['nir']["coefficients"][1]],
#             'Coef3 nir':[seq['nir']["coefficients"][2]],
#             'Coef1 ndvi':[seq['ndvi']["coefficients"][0]],
#             'Coef2 ndvi':[seq['ndvi']["coefficients"][1]],
#             'Coef3 ndvi':[seq['ndvi']["coefficients"][2]],
#         })
#     return pixel


# now=time.time()
# createClassifier()
# later=time.time()-now
# print("completed in {}".format(later))

# from sklearn.ensemble import RandomForestRegressor
# #from sklearn.model_selection import train_test_split

# # Here array_train is a vector that holds all the explanatory variable data (the column is the number of features) + the target variable/label (last index - array_train[:, -1])
# dataframe = pd.DataFrame(array_train[:, :], columns=names_r)
# # Divide into attributes (explanatory variables) and labels (what to predict)
# array = dataframe.values
# X = array[:, 0 : nf - 1]
# y = array[:, nf - 1]
# # If you need to split into training and test dataset
# # X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=0)
# regressor = RandomForestRegressor(n_estimators=150, random_state=0, n_jobs=-1)

# # For prediction
# y_pred = regressor.predict(X_train)
createClassifier()