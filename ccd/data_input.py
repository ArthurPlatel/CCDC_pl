import numpy as np
from osgeo import gdal
import glob, os
from datetime import date
import time
from ccd import detect
#from parameters import defaults as dfs


def image(x_diviser, final):
    n=0
    t=0
    image=gdal.Open(final[0])
    shape=image.ReadAsArray().shape
    x_pixels=shape[1]
    x_value=int(x_pixels/x_diviser)
    y_pixels=shape[2]
    remainder=x_pixels%x_diviser
    time_series={"dict"+str(k):{(x,y):{'dates':[],'blue':[],'green':[], 'red':[], 'nir':[],'ndvi':[],'qas':[]} for y in range(y_pixels) for x in range(k*x_value,(k+1)*x_value)} for k in range(x_diviser)}
    for fusion_tif in final:
        start_time = time.time()
        print("processing image {} of {}".format(n, len(final)-1))
        image=gdal.Open(fusion_tif)
        o_date = date(int(fusion_tif[-14:-10]), int(fusion_tif[-9:-7]), int(fusion_tif[-6:-4]))
        gordinal = o_date.toordinal()
        b_ras = image.GetRasterBand(1).ReadAsArray()
        g_ras = image.GetRasterBand(2).ReadAsArray()
        r_ras = image.GetRasterBand(3).ReadAsArray()
        n_ras = image.GetRasterBand(4).ReadAsArray()
        i_shape=image.ReadAsArray().shape
        for k in range(x_diviser):
            for x in range((k*x_value),(k+1)*x_value):
                for y in range(i_shape[2]):
                    time_series["dict"+str(k)][x,y]["dates"].append(gordinal)
                    time_series["dict"+str(k)][x,y]["blue"].append(b_ras[x,y]) 
                    time_series["dict"+str(k)][x,y]["green"].append(g_ras[x,y])  
                    time_series["dict"+str(k)][x,y]["red"].append(r_ras[x,y])  
                    time_series["dict"+str(k)][x,y]["nir"].append(n_ras[x,y])    
                    time_series["dict"+str(k)][x,y]["ndvi"].append((n_ras[x,y]-r_ras[x,y])/(n_ras[x,y]+r_ras[x,y]))
                    time_series["dict"+str(k)][x,y]["qas"].append(1) 
        n+=1 
        image_time=time.time()-start_time
        t+=image_time
        print("image completed in: {} for a total time of {} min \n".format(image_time,(t/3600)))
    final_dict ={}
    for d in time_series:
        print("merging dict:{}".format(d))
        final_dict = final_dict|time_series[str(d)]
        print("Merge Complete")
    return final_dict


def pixel_array(pixel_x,pixel_y, dict):
    pixel=dict[pixel_x,pixel_y]
    return np.array([pixel['dates'], pixel['blue'], pixel['green'], pixel['red'], pixel['nir'], pixel['ndvi'],pixel['qas']])

def getAll(final):
    # files = glob.glob(os.path.join(parent_dir, '*.tif'))
    # final = sorted(files)
    ts1=image(30,final)
    return ts1
   
def to_dict(c1,c0,x_pixels,final):
        time_series={(x,y):{'dates':[],'blue':[],'green':[], 'red':[], 'nir':[],'ndvi':[],'qas':[]} for y in range(c0,c1) for x in range(x_pixels)} 
        n=0
        t=0
        for fusion_tif in final:
            start_time = time.time()
            if n%100==0:
                print("processing image {} of {}".format(n, len(final)-1))
            image=gdal.Open(fusion_tif)
            gordinal = date(int(fusion_tif[-14:-10]), int(fusion_tif[-9:-7]), int(fusion_tif[-6:-4])).toordinal()
            b_ras = image.GetRasterBand(1).ReadAsArray()
            g_ras = image.GetRasterBand(2).ReadAsArray()
            r_ras = image.GetRasterBand(3).ReadAsArray()
            n_ras = image.GetRasterBand(4).ReadAsArray()
            i_shape=image.ReadAsArray().shape
            for x in range(x_pixels):
                for y in range(c0,c1):
                    time_series[x,y]["dates"].append(gordinal)
                    time_series[x,y]["blue"].append(b_ras[x,y]) 
                    time_series[x,y]["green"].append(g_ras[x,y])  
                    time_series[x,y]["red"].append(r_ras[x,y])  
                    time_series[x,y]["nir"].append(n_ras[x,y])    
                    time_series[x,y]["ndvi"].append((n_ras[x,y]-r_ras[x,y])/(n_ras[x,y]+r_ras[x,y]))
                    time_series[x,y]["qas"].append(1) 
            image_time=time.time()-start_time
            t+=image_time
            if n%100==0:
                print("image completed in: {} for a total time of {} min \n".format(image_time,(t/3600)))
            n+=1
            if n==len(final)-1:
                print("Merging Dictionaries")
        return time_series

def Allto_dict(c0,c1,x_pixels,final):
        time_series={(x,y):{'dates':[],'blue':[],'green':[], 'red':[], 'nir':[],'ndvi':[],'qas':[]} for y in range(c0,c1) for x in range(x_pixels)} 
        n=0
        t=0
        for fusion_tif in final:
            start_time = time.time()
            if n%100==0:
                print("processing image {} of {}".format(n, len(final)-1))
            image=gdal.Open(fusion_tif)
            gordinal = date(int(fusion_tif[-14:-10]), int(fusion_tif[-9:-7]), int(fusion_tif[-6:-4])).toordinal()
            b_ras = image.GetRasterBand(1).ReadAsArray()
            g_ras = image.GetRasterBand(2).ReadAsArray()
            r_ras = image.GetRasterBand(3).ReadAsArray()
            n_ras = image.GetRasterBand(4).ReadAsArray()
            i_shape=image.ReadAsArray().shape
            for x in range(x_pixels):
                for y in range(c0,c1):
                    time_series[x,y]["dates"].append(gordinal)
                    time_series[x,y]["blue"].append(b_ras[x,y]) 
                    time_series[x,y]["green"].append(g_ras[x,y])  
                    time_series[x,y]["red"].append(r_ras[x,y])  
                    time_series[x,y]["nir"].append(n_ras[x,y])    
                    time_series[x,y]["ndvi"].append((n_ras[x,y]-r_ras[x,y])/(n_ras[x,y]+r_ras[x,y]))
                    time_series[x,y]["qas"].append(1) 
            image_time=time.time()-start_time
            t+=image_time
            n+=1
            if n%100==0:
                print("image completed in: {} for a total time of {} min \n".format(image_time,(t/3600)))
            
        return time_series


def Allagain(final):
    n=0
    shape=[4,686,976] #[4,641,689]
    time_series=[[[[]for r in range(7)]for y in range(shape[2])]for x in range(shape[1])]
    modulo=shape[1]%100
    end=shape[1]-modulo
    for fusion_tif in final:
        start_time = time.time()
        n+=1
        print("processing image {} of {}".format(n, len(final)))
        image=gdal.Open(fusion_tif)
        gordinal = date(int(fusion_tif[-14:-10]), int(fusion_tif[-9:-7]), int(fusion_tif[-6:-4])).toordinal()
        b_ras = image.GetRasterBand(1).ReadAsArray()
        g_ras = image.GetRasterBand(2).ReadAsArray()
        r_ras = image.GetRasterBand(3).ReadAsArray()
        n_ras = image.GetRasterBand(4).ReadAsArray()
        shape=image.ReadAsArray().shape
        for k in range(int(end/100)):
            for x in range(shape[1]):
                #print("processing row {}: of {}".format(x,shape[1]))
                for y in range(k*100,k+1*100):
                    time_series[x][y][0].append(gordinal)
                    time_series[x][y][1].append(b_ras[x,y]) 
                    time_series[x][y][2].append(g_ras[x,y])  
                    time_series[x][y][3].append(r_ras[x,y])  
                    time_series[x][y][4].append(n_ras[x,y])    
                    time_series[x][y][5].append((n_ras[x,y]-r_ras[x,y])/(n_ras[x,y]+r_ras[x,y]))
                    time_series[x][y][6].append(1)
                    # if n==len(final):
                    #     data = np.array(time_series[x][y])
                    #     dates,blues,greens,reds,nirs,ndvis,qas=data
                    #     result=detect(dates, greens,blues, reds, nirs, ndvis, qas, dfs['params'])
                    #     ccd_data[x][y].append(result)
        for x in range(shape[1]):
            for y in range(end,shape[2]):
                    time_series[x][y][0].append(gordinal)
                    time_series[x][y][1].append(b_ras[x,y]) 
                    time_series[x][y][2].append(g_ras[x,y])  
                    time_series[x][y][3].append(r_ras[x,y])  
                    time_series[x][y][4].append(n_ras[x,y])    
                    time_series[x][y][5].append((n_ras[x,y]-r_ras[x,y])/(n_ras[x,y]+r_ras[x,y]))
                    time_series[x][y][6].append(1)
                    # if n==len(final):
                    #     data = np.array(time_series[x][y])
                    #     dates,blues,greens,reds,nirs,ndvis,qas=data
                    #     result=detect(dates, greens,blues, reds, nirs, ndvis, qas, dfs['params'])
                    #     ccd_data[x][y].append(result)
            image_time=time.time()-start_time
            #if n%100==0:
        print("image completed in: {} \n".format(image_time))
    return time_series #time_series






def Allto_listdict(final):
    n=0
    shape=[4,686,976]
    time_series=[{(x,y):{'dates':[],'blue':[],'green':[], 'red':[], 'nir':[],'ndvi':[],'qas':[]} for y in range(shape[2])} for x in range(shape[1])]
    modulo=shape[1]%100
    end=shape[1]-modulo
    for fusion_tif in final:
        start_time = time.time()
        n+=1
        #time_series=[{(x,y):{'dates':[],'blue':[],'green':[], 'red':[], 'nir':[],'ndvi':[],'qas':[]}for x in range(shape[1])}for y in range(shape[2])]
        #if n%100==0:
        print("processing image {} of {}".format(n, len(final)))
        image=gdal.Open(fusion_tif)
        gordinal = date(int(fusion_tif[-14:-10]), int(fusion_tif[-9:-7]), int(fusion_tif[-6:-4])).toordinal()
        b_ras = image.GetRasterBand(1).ReadAsArray()
        g_ras = image.GetRasterBand(2).ReadAsArray()
        r_ras = image.GetRasterBand(3).ReadAsArray()
        n_ras = image.GetRasterBand(4).ReadAsArray()
        shape=image.ReadAsArray().shape
        #time_series=[{(x,y):{'dates':[],'blue':[],'green':[], 'red':[], 'nir':[],'ndvi':[],'qas':[]}for x in range(shape[1])}for y in range(shape[2])]
        for k in range(int(end/100)):
            for x in range(shape[1]):
                #print("processing row {}: of {}".format(x,shape[1]))
                for y in range(k*100,k+1*200):
                    time_series[x][x,y]["dates"].append(gordinal)
                    time_series[x][x,y]["blue"].append(b_ras[x,y]) 
                    time_series[x][x,y]["green"].append(g_ras[x,y])  
                    time_series[x][x,y]["red"].append(r_ras[x,y])  
                    time_series[x][x,y]["nir"].append(n_ras[x,y])    
                    time_series[x][x,y]["ndvi"].append((n_ras[x,y]-r_ras[x,y])/(n_ras[x,y]+r_ras[x,y]))
                    time_series[x][x,y]["qas"].append(1)
        for x in range(shape[1]):
            for y in range(end,shape[2]):
                    time_series[x][x,y]["dates"].append(gordinal)
                    time_series[x][x,y]["blue"].append(b_ras[x,y]) 
                    time_series[x][x,y]["green"].append(g_ras[x,y])  
                    time_series[x][x,y]["red"].append(r_ras[x,y])  
                    time_series[x][x,y]["nir"].append(n_ras[x,y])    
                    time_series[x][x,y]["ndvi"].append((n_ras[x,y]-r_ras[x,y])/(n_ras[x,y]+r_ras[x,y]))
                    time_series[x][x,y]["qas"].append(1)

            image_time=time.time()-start_time
            #if n%100==0:
        print("image completed in: {} \n".format(image_time))
    return time_series #time_series

def Allto_list(final):
    n=0
    shape=[4,686,976]
    ccd=np.zeros(shape[1],shape[2])
    #time_series={(x,y):{'dates':[],'blue':[],'green':[], 'red':[], 'nir':[],'ndvi':[],'qas':[]} for y in range(shape[2]) for x in range(shape[1])}
    time_series2=[{(x,y):{'dates':[],'blue':[],'green':[], 'red':[], 'nir':[],'ndvi':[],'qas':[]}for x in range(shape[1])}for y in range(200)]
    time_series3=[{(x,y):{'dates':[],'blue':[],'green':[], 'red':[], 'nir':[],'ndvi':[],'qas':[]}for x in range(shape[1])}for y in range(200)]
    time_series4=[{(x,y):{'dates':[],'blue':[],'green':[], 'red':[], 'nir':[],'ndvi':[],'qas':[]}for x in range(shape[1])}for y in range(200)]
    list=[]
    modulo=shape[1]%200
    end=shape[1]-modulo
    for fusion_tif in final:
        start_time = time.time()
        n+=1
        #time_series=[{(x,y):{'dates':[],'blue':[],'green':[], 'red':[], 'nir':[],'ndvi':[],'qas':[]}for x in range(shape[1])}for y in range(shape[2])]
        #if n%100==0:
        print("processing image {} of {}".format(n, len(final)))
        image=gdal.Open(fusion_tif)
        gordinal = date(int(fusion_tif[-14:-10]), int(fusion_tif[-9:-7]), int(fusion_tif[-6:-4])).toordinal()
        b_ras = image.GetRasterBand(1).ReadAsArray()
        g_ras = image.GetRasterBand(2).ReadAsArray()
        r_ras = image.GetRasterBand(3).ReadAsArray()
        n_ras = image.GetRasterBand(4).ReadAsArray()
        shape=image.ReadAsArray().shape
        #time_series=[{(x,y):{'dates':[],'blue':[],'green':[], 'red':[], 'nir':[],'ndvi':[],'qas':[]}for x in range(shape[1])}for y in range(shape[2])]
        for k in range(int(end/200)):
            for x in range(shape[1]):
                #print("processing row {}: of {}".format(x,shape[1]))
                for y in range(k*100,k+1*200):
                    time_series[x,y]["dates"].append(gordinal)
                    time_series[x,y]["blue"].append(b_ras[x,y]) 
                    time_series[x,y]["green"].append(g_ras[x,y])  
                    time_series[x,y]["red"].append(r_ras[x,y])  
                    time_series[x,y]["nir"].append(n_ras[x,y])    
                    time_series[x,y]["ndvi"].append((n_ras[x,y]-r_ras[x,y])/(n_ras[x,y]+r_ras[x,y]))
                    time_series[x,y]["qas"].append(1)
                    if k+1*200==end:
                        d=time_series[x,y]
                        data = np.array([d['dates'],d['blue'],d["green"],d['red'],d['nir'],d['ndvi'],d['qas']])
                        dates,blues,greens,reds,nirs,ndvis,qas = data
                        result=ccd.detect(dates, greens,blues, reds, nirs, ndvis, qas, dfs['params'])

        for x in range(shape[1]):
            for y in range(end,shape[2]):
                    time_series[x,y]["dates"].append(gordinal)
                    time_series[x,y]["blue"].append(b_ras[x,y]) 
                    time_series[x,y]["green"].append(g_ras[x,y])  
                    time_series[x,y]["red"].append(r_ras[x,y])  
                    time_series[x,y]["nir"].append(n_ras[x,y])    
                    time_series[x,y]["ndvi"].append((n_ras[x,y]-r_ras[x,y])/(n_ras[x,y]+r_ras[x,y]))
                    time_series[x,y]["qas"].append(1)

            image_time=time.time()-start_time
            #if n%100==0:
        print("image completed in: {} \n".format(image_time))
            
    return time_series #time_series


def line(image_dir,lines):
    files = glob.glob(os.path.join(image_dir, '*.tif'))
    final = sorted(files)
    image=gdal.Open(final[0])
    shape=image.ReadAsArray().shape
    x_pixels=shape[1]
    y_pixels=shape[2]
    output={}
    kp=0
    print(y_pixels)
    first=True
    for k in range(y_pixels):
        if first:
            first=False
        else:
            if k ==(y_pixels-1) or k%lines==0:
                start_time = time.time()
                output=output|to_dict(k,x_pixels,kp,final)
                #to_dict(k,x_pixels,kp,final)
                kp=k
                image_time=time.time()-start_time
                print("finished line {} of {} \n in {}".format(k,y_pixels,image_time))
    return output


def sortImages(parent_dir,odd=False):
    files = glob.glob(os.path.join(parent_dir, '*.tif'))
    if odd==True:
        odd=[]
        for image in files:
            if (int(image[-5:-4])%2) != 0:
                odd.append(image)
        images = sorted(odd)
    else:
        images=sorted(files)
    return images


def get_data(parent_dir,pixel_x,pixel_y,odd=False):
    #initialize empty lists
    o_time = []
    greens = []
    blues = []
    reds = []
    nirs = []
    ndvis = []
    qas = []
    qa = 0
    #files = glob.glob(os.path.join(parent_dir, '*.tif'))
    #final = sorted(files)
    final=sortImages(parent_dir,odd)
    for fusion_tif in final:
        image=gdal.Open(fusion_tif)
        year = int(fusion_tif[-14:-10])
        month = int(fusion_tif[-9:-7])
        day = int(fusion_tif[-6:-4])
        o_date = date(year, month, day)
        gordinal = o_date.toordinal()
        o_time.append(gordinal)
        #arrayLen = len(image.GetRasterBand(1).ReadAsArray())
        blue = float(image.GetRasterBand(1).ReadAsArray()[pixel_x][pixel_y])
        blues.append(blue)
        green = float(image.GetRasterBand(2).ReadAsArray()[pixel_x][pixel_y])
        greens.append(green)
        red = float(image.GetRasterBand(3).ReadAsArray()[pixel_x][pixel_y])
        reds.append(red)
        nir = float(image.GetRasterBand(4).ReadAsArray()[pixel_x][pixel_y])
        nirs.append(nir)
        ndvi = ((nir-red)/(red+nir)*1000)
        ndvis.append(ndvi)
        qas.append(qa)
    return np.array([o_time,blues,greens,reds,nirs,ndvis,qas])
    # print(arrayLen)
    # print("Origin = ({}, {})".format(geotransform[0], geotransform[3]))
    # print("Pixel Size = ({}, {})".format(geotransform[1], geotransform[5]))
    # print("x pixels: {}, y pixels: {}, and total # bands: {}".format(xpixels,ypixels,bands))
    # print(xpixels*ypixels)
    
# # pixel_x = 10
# # pixel_y = 10
# # parent_dir = '/Users/arthur.platel/PF-SR/'

pix=0
# # get_data(parent_dir,pixel_x,pixel_y)

def CCD(x):
    global pix 
    print("ccding pixel",pix)   
    pix+=1
    data=np.array(x)
    dates,blues,greens,reds,nirs,ndvis,qas=data
    result=detect(dates, greens,blues, reds, nirs, ndvis, qas, dfs['params'])
    return result

def detectAll(inputArray):
    now=time.time()
    new=[[[CCD(inputArray[x][y])]for y in range(len(inputArray[1]))] for x in range(len(inputArray))]
    print("completed ccding in:", time.time()-now)
    return new

def detectRow(inputArray, r0, r1):
    rows,cols,bands=np.shape(inputArray)
    array=[[[(CCD(inputArray[x][y])["change_models"])]for y in range(cols)]for x in range(r0,r1)]
    # for x in range(r0,r1):
    #     for y in range(cols):
    #         array[x][y]=(CCD(inputArray[x][y])["change_models"])
    return array 
    
    
        
        # for y in range(len(inputArray[0])):
        #     data = np.array(inputArray[x][y])
        #     dates,blues,greens,reds,nirs,ndvis,qas=data
        #     result=detect(dates, greens,blues, reds, nirs, ndvis, qas, dfs['params'])
        #     outputArray[x][y].append(result)
        
