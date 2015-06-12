import numpy as np
import pandas as pd
from sklearn import preprocessing

#sudo apt-get install python-pip python-dev build-essential  
#sudo apt-get install libatlas-base-dev gfortran
#
#sudo pip install numpy
#sudo pip install scipy
#sudo pip install matplotlib
#sudo pip install -U scikit-learn
#sudo pip install pandas
    
if __name__ == "__main__":
    
    # parameters
    fill_cat_na_with_more_frequent = False;
    fill_cat_na_with_no_cat = False;
    fill_int_na_with_mean = True;
    K = 1;
    
    # read file    
    file = pd.read_csv("truncate.csv", nrows = 50);
    
    # target
    y = file.Label;
    
    # numeric features 
    X = file.iloc[:,2:15];
    
    # Dummyfies category columns in K categories    
    for col in file.iloc[:,15:41].columns: # for all columns from C1 to C26
        
        # build a catalog of encountered categories ordered by frequency
        catalog = {}        
        for word in file[col].values:
            catalog[word] = catalog.get(word, 0) + 1;
        catalog = sorted(catalog.items(), key=lambda item: item[1], reverse=True)
    
        # initialize NaN categories with "more frequent category" or none
        if (fill_cat_na_with_more_frequent):
            # remove NaN from more frequent categories
            catalog.pop(np.nan, None);
            file[col].fillna(catalog[0][0]);
        elif (fill_cat_na_with_no_cat):
            # remove NaN from more frequent categories
            catalog.pop(np.nan, None);
    
        for i in range(0, min(K, len(catalog))): 
            s = None;
            if (isinstance(catalog[i][0], basestring)):
                s = file[col].eq(catalog[i][0])                
            else:
                s = file[col].isnull();                                
            s.name = col + "_" + str(catalog[i][0]);
            X = pd.concat([X, s.astype(int)], axis=1);
    
    # input numerical columns range(0, 13) with median/mean value.
    if (fill_int_na_with_mean):
        X = X.fillna(X.mean());
    else:
        X = X.fillna(X.median());

    # scale data and normalize data.
    X = (X - X.mean()) / (X.max() - X.min());
    
    print(X);    

