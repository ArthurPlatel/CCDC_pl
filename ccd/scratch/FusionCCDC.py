from typing import final
import FusionFunctions as FF
from parameters import defaults as dfs
import numpy as np
import time
import CCDC_Final as data


# images=FF.sortImages(dfs["parent_dir"])
# shape=FF.shape(images)
# allData=data.pixelData(images)
# rows,cols,bands=np.shape(allData)
# out_array=[[[]for y in range(cols)]for x in range(rows)]
# row_Zero=data.detectRow(allData,out_array,0,1)
# change_array=np.zeros((rows,cols))

# for k in range(len(row_Zero[0])):
#     if len(row_Zero[0][k])>1:
#         change_array[0][k]=1

#FF.save_raster2(dfs["out_dir"],1,change_array,images, "Change")


# from ccd.data_input import Allto_list
# from typing import final
# import FusionFunctions as FF
# from parameters import defaults as dfs
# import numpy as np
# import time
# import CCDC_Final as data

# # start=time.time()
# images=FF.sortImages(dfs["parent_dir"])
# shape=FF.shape(images)
# # NewArray =[np.zeros(shape[1:3])for k in range(2)]
# # for k in range(7):
# #     row_start=time.time()
# #     FF.classification_ccd(0,1,shape[1],images,NewArray,736696,737971)
# #     FF.save_raster(dfs["out_dir"],736696,2,NewArray,images,"LandClass")
# #     now=time.time()
# #     print("finished row {} in {}".format(k,now-row_start))
# # now=time.time()
# # print("finished all {} in {}".format(k,now-start))

# #dict = Allto_dict(0,1,shape[1],images)
# #ccd_data=[[[]for x in range(shape[2])]for y in range(shape[1])]
# now=time.time()
# allData=data.Allagain(images)
# later=time.time()
# total=later-now
# print("total time for ingestion was:",total)
# now2=time.time()
# data.classify(allData)
# ("total classification in:",time.time()-now2)
# # for k in range(5):
# #     print("processing line {}".format(k))
# #     list.append(data.Allto_dict(k,k+1,shape[1],images))
# #print(two[100][100][3])

