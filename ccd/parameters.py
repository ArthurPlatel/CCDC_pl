############################
# Default Configuration Options
############################


defaults = {
# Most important CCDC Variables
    'nth':1,    #use every nth image from image stack for analysis, nth=1 if every image should be used
    'pool_size':4, #number of multiprocessing pools to use in CCDC analysis
    'num_rows': 5, #number of pixel rows to process at a time
    'MEOW_SIZE': 12, #minimum expected observation window needed to produce a fit.
    'PEEK_SIZE': 8, #number of repetive observations that must surpass change threshold to detect change
    'DAY_DELTA': 365, #number of days required for a years worth of data, defined to be 365
    'AVG_DAYS_YR':365.2425,
    'OUTLIER_THRESHOLD':25,
    'CHANGE_THRESHOLD':15,
    'T_CONST': 4.89,  
    #assuming there is imagery every day, vari =480/dfs['nth']                                                                        
    'vari':300,

    ############################
    # Define spectral band indices on input observations array
    ############################
    'BLUE_IDX': 0,
    'GREEN_IDX': 1,
    'RED_IDX': 2,
    'NIR_IDX': 3,
    'NDVI': 4,
    'NDWI': 5,
    'QA_IDX': 6,

    # Spectral bands that are utilized for detecting change
    'DETECTION_BANDS': [4],

    # Spectral bands that are utilized for Tmask filtering
    'TMASK_BANDS': [1,2,3,4],

    'CCD_bands':["blue","green","red","nir","ndvi","ndwi"],

#######not in use as of now########
######### will update when continue classification######
    ##############################
   # Classification Settings
   ##############################
     # shp file containing training data for classification
    #csv file to create training raster
    'n_estimators':1000,
    'oob_score':True,

     ############################
    # Values related to model fitting
    ############################
    'FITTER_FN': 'ccd.models.lasso.fitted_model',
    'LASSO_MAX_ITER': 3000,

    # 2 for tri-modal; 2 for bi-modal; 2 for seasonality; 2 for linear
    'COEFFICIENT_MIN': 4,
    'COEFFICIENT_MID': 6,
    'COEFFICIENT_MAX': 8,

    # Value used to determine the minimum number of observations required for a
    # defined number of coefficients
    # e.g. COEFFICIENT_MIN * NUM_OBS_FACTOR = 12
    'NUM_OBS_FACTOR': 3,


    

    ############################
    # Representative values in the QA band
    ############################
    'params': {'QA_BITPACKED': False,
                'QA_FILL': 255,
                'QA_CLEAR': 0,
                'QA_WATER': 1,
                'QA_SHADOW': 2,
                'QA_SNOW': 3,
                'QA_CLOUD': 4},

    'QA_BITPACKED': True,
    'QA_FILL': 0,
    'QA_CLEAR': 1,
    'QA_WATER': 2,
    'QA_SHADOW': 3,
    'QA_SNOW': 4,
    'QA_CLOUD': 5,
    'QA_CIRRUS1': 8,
    'QA_CIRRUS2': 9,
    'QA_OCCLUSION': 10,

    ############################
    # Representative values for the curve QA
    ############################
    'CURVE_QA': {
        'PERSIST_SNOW': 54,
        'INSUF_CLEAR': 44,
        'START': 14,
        'END': 24},

    ############################
    # Threshold values used
    ############################
    'CLEAR_OBSERVATION_THRESHOLD': 5,
    'CLEAR_PCT_THRESHOLD': 0.25,
    'SNOW_PCT_THRESHOLD': 0.75,
    

    # Value added to the median green value for filtering purposes
    #'MEDIAN_GREEN_FILTER': 400,

    


}
