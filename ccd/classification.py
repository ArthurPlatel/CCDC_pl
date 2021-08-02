#Imports for classification
from osgeo import gdal
import numpy as np
import geopandas as gpd
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from ccd import parameters


#################################
##########Classification#########
# Function to create classifier
def createClassifier():
    gdf = gpd.read_file(parameters.defaults['trainingData'])
    class_names = gdf["layer"].unique()
    class_ids=gdf['id'].unique()
    print("\nBuilding Random Forest Classifier")
    print("LandClasses: {} With values {}\n".format(class_names,class_ids))
    data=pd.DataFrame({
        #'RMSE blue':[gdf.iloc[k][2]for k in range(len(gdf))],
        #'RMSE green':[gdf.iloc[k][3]for k in range(len(gdf))],
        'RMSE red':[gdf.iloc[k][4]for k in range(len(gdf))],
        'RMSE nir':[gdf.iloc[k][5]for k in range(len(gdf))],
        'RMSE ndvi':[gdf.iloc[k][6]for k in range(len(gdf))],
        #'Coef1 blue':[gdf.iloc[k][7]for k in range(len(gdf))],
        #'Coef1 green':[gdf.iloc[k][10]for k in range(len(gdf))],
        'Coef1 red':[gdf.iloc[k][13]for k in range(len(gdf))],
        'Coef1 nir':[gdf.iloc[k][16]for k in range(len(gdf))],
        'Coef1 ndvi':[gdf.iloc[k][19]for k in range(len(gdf))],
        'landcover':[gdf.iloc[k][0]for k in range(len(gdf))]
    })

    X=data[['RMSE red','RMSE nir','RMSE ndvi','Coef1 red','Coef1 nir', 'Coef1 ndvi']]  #'Coef1 blue','Coef1 green','RMSE blue','RMSE green' Features
    y=data['landcover']  # Labels

    # Split dataset into training set and test set
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3) # 70% training and 30% test

    #Create a Gaussian Classifier
    clf=RandomForestClassifier(parameters.defaults["n_estimators"],oob_score=parameters.defaults['oob_score'])

    #Train the model using the training sets y_pred=clf.predict(X_test)
    clf.fit(X_train,y_train)
    y_pred=clf.predict(X_test)
    accuracy = accuracy_score(y_test,y_pred)
    #Print accuracy results
    if parameters.defaults['oob_score']:
        print(f'Out-of-bag score estimate: {clf.oob_score_:.3}')
        print(f'Mean accuracy score: {accuracy:.3}')
    print("Classifier Complete \n")
    return clf

        #Classification function


def classify(df):
    clf=createClassifier()
    class_pred=clf.predict(df)[0]
    return class_pred

#helper function to load CCD result into readable dataframe for classification
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



    