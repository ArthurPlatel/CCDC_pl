from osgeo import gdal
import numpy as np
import ogr
import geopandas as gpd
import pandas as pd

training = '/Users/arthur.platel/Desktop/Fusion_Images/CZU/PF-SR/2021-06-30.tif'
training_ds= gdal.Open(training)
cols=training_ds.RasterYSize
rows=training_ds.RasterXSize
gdf = gpd.read_file('/Users/arthur.platel/Desktop/Training/TrainingData63021.shp')
class_names = gdf["layer"].unique()
class_ids=gdf['id'].unique()
class_data=gdf['RMSE_1']



data=[[gdf.iloc[k][l]for l in range(2, len(gdf.iloc[0])-2)]for k in range(len(gdf))]
target=np.array([[int(gdf.iloc[k][0])]for k in range(len(gdf))])

train={"data":data,'target':target,'target_names':class_names}
print(train)

data=pd.DataFrame({
    'sepal length':train.data[:,0],
    'sepal width':train.data[:,1],
    'petal length':train.data[:,2],
    'petal width':train.data[:,3],
    'species':train.target
})

# #print(target)
# data=pd.DataFrame({
#     'RMSE Blue':[[gdf.iloc[k][3]for k in range(len(gdf))]],
#     'RMSE Green':[[gdf.iloc[k][4]for k in range(len(gdf))]],
#     'RMSE Red':[[gdf.iloc[k][4]for k in range(len(gdf))]],
#     'RMSE Nir':[[gdf.iloc[k][4]for k in range(len(gdf))]],
#     'RMSE Ndvi':[[gdf.iloc[k][4]for k in range(len(gdf))]],
#     'RMSE Nir':[[gdf.iloc[k][4]for k in range(len(gdf))]],
#     'species':[[gdf.iloc[k][0]for k in range(len(gdf))]]
# })
# print(data.head)
#print(len(gdf))
#print(gdf.iloc[:1,3])
#print(class_names)
traindata = zip(gdf['layer'],gdf['id'])
#print(traindata)
df=pd.DataFrame({"label":class_names, "id":class_ids})
#print(df)