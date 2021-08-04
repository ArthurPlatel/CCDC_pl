############################
# Default Configuration Options
############################
defaults = {
     #'parent_dir':'/Users/arthur.platel/Desktop/Fusion_Images/SantaCruz_NoFire',
    #'parent_dir':'/Users/arthur.platel/Desktop/Fusion_Images/deforestation_Octave/PF-SR',
    #'parent_dir': '/Users/arthur.platel/Desktop/Fusion_Images/CZU_FireV2',
    #'parent_dir':'/Users/arthur.platel/Desktop/Fusion_Images/Imperial_Subset',
    #'parent_dir': '/Users/arthur.platel/Desktop/Fusion_Images/SantaCruz_NoFire',
    #'out_dir':"/Users/arthur.platel/Desktop/CCDC_Output/",
    'out_dir':'/home/arthur.platel/ccd_out/sandypointV3',
    'parent_dir':'/home/arthur.platel/FusionImages/sandypointV3/ccd-sandy-point-ca-v3/site-1/PF-SR',

    'MEOW_SIZE': 12,
    'PEEK_SIZE': 8,
    'DAY_DELTA': 365,
    'AVG_DAYS_YR': 365.2425,

    # 2 for tri-modal; 2 for bi-modal; 2 for seasonality; 2 for linear
    'COEFFICIENT_MIN': 4,
    'COEFFICIENT_MID': 6,
    'COEFFICIENT_MAX': 8,

    # Value used to determine the minimum number of observations required for a
    # defined number of coefficients
    # e.g. COEFFICIENT_MIN * NUM_OBS_FACTOR = 12
    'NUM_OBS_FACTOR': 3,

    ############################
    # Define spectral band indices on input observations array
    ############################
    'BLUE_IDX': 0,
    'GREEN_IDX': 1,
    'RED_IDX': 2,
    'NIR_IDX': 3,
    'NDVI': 4,
    'QA_IDX': 5,

    # Spectral bands that are utilized for detecting change
    'DETECTION_BANDS': [1,2,3,4],

    # Spectral bands that are utilized for Tmask filtering
    'TMASK_BANDS': [1,2,3,4],

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
    # original CFMask values
    #QA_FILL: 255
    #QA_CLEAR: 0
    #QA_WATER: 1
    #QA_SHADOW: 2
    #QA_SNOW: 3
    #QA_CLOUD: 4
    # ARD bitpacked offsets
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
    'OUTLIER_THRESHOLD': 40.888186879610423,
    'CHANGE_THRESHOLD': 25.086272469388987,
    'T_CONST': 4.89,
    'vari':80,

    # Value added to the median green value for filtering purposes
    #'MEDIAN_GREEN_FILTER': 400,

    ############################
    # Values related to model fitting
    ############################
    'FITTER_FN': 'ccd.models.lasso.fitted_model',
    'LASSO_MAX_ITER': 1000,
    

    ##############################
   # Classification Settings
   ##############################

    'trainingData':'/Users/arthur.platel/Desktop/Training/TrainingData63021.shp',
    'n_estimators':1000,
    'oob_score':True,

}

   