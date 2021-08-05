import time
from ccd.CCD_Pool import imageCCD


def main(parent_dir):
    timelist=[]
    for k in range(4):
        now1=time.time()
        imageCCD(parent_dir,"na",size=k+1,test=True,print=False)
        thread_1=time.time()-now1
        timelist.append(thread_1)
    for k in range(4):
        print("CCD code using {} threads processed 20 rows in {}".format(k+1,timelist[k]))

if __name__ == '__main__':
    parent_dir='/Users/arthur.platel/Desktop/Fusion_Images/CZU_FireV2'
    main(parent_dir)

