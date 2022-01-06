import numpy as np
import pandas as pd

import Logger
logger = Logger.Logger()
df_train = pd.read_csv("F:\\flight\\flight243-main\\Data_Train.csv")

'''check for null values'''
df_train.isna().sum()

'''after dropping the Nan values we see the shape is almost same'''
df_train.dropna(inplace =True)

df_train['Day_of_travel']=pd.to_datetime(df_train["Date_of_Journey"],format = '%d/%m/%Y').dt.day
df_train['Month_of_travel']=pd.to_datetime(df_train["Date_of_Journey"],format = '%d/%m/%Y').dt.month

df_train.drop(["Date_of_Journey"],axis = 1, inplace = True)

'''Extracting the departure time and min'''
df_train['Dep_hour']=pd.to_datetime(df_train['Dep_Time']).dt.hour
df_train['Dep_min']=pd.to_datetime(df_train['Dep_Time']).dt.minute

df_train.drop(['Dep_Time'], axis = 1, inplace = True)

'''Extracting the arrival time and min'''
df_train['Arr_hour']=pd.to_datetime(df_train['Arrival_Time']).dt.hour
df_train['Arr_min']=pd.to_datetime(df_train['Arrival_Time']).dt.minute

df_train.drop(['Arrival_Time'], axis = 1, inplace = True)

'''creating a list with the duration column '''
duration = list(df_train["Duration"])
'''iterating the loop so that whichever line does have mins adding 0h or 0m at end/start acordingly.'''
for i in range(len(duration)):
    if len(duration[i].split()) != 2:
        if "h" in duration[i]:
            duration[i] = duration[i].strip() + " 0m"   ##removing unwanted spaces
        else:
            duration[i] = "0h " + duration[i]

'''creating column and separating as mins.'''

duration_mins = []
duration_hours=[]
for i in range(len(duration)):
    duration_hours.append(int(duration[i].split(sep = "h")[0]))    # Extract hours from duration
    duration_mins.append(int(duration[i].split(sep = "m")[0].split()[-1]))

df_train['Duration_hrs'] = duration_hours
df_train['Duration_mins'] = duration_mins

df_train.drop(['Duration'], axis = 1, inplace = True)

'''One hot endcoding on the Airline data'''
Airline = df_train['Airline']
Airline = pd.get_dummies(Airline)

'''check no of values in source'''
df_train['Source'].value_counts()

'''apply one hot encoding on source column '''
Source = df_train[['Source']]
Source = pd.get_dummies(Source, drop_first = True)


'''apply one hot encoding on destination column'''
Destination = df_train[['Destination']]
Destination = pd.get_dummies(Destination, drop_first = True)

'''dropping the route column as its of no use and we have total stops to work out.'''
df_train.drop(['Route','Additional_Info'],axis = 1, inplace = True)

'''changing the values in the total_stops column into numbers'''
df_train['Total_Stops'].replace({'non-stop':0,'1 stop':1,'2 stops':2,'3 stops':3,'4 stops':4}, inplace =True)

'''creating the final train dataset'''
df1_train = pd.concat([df_train, Airline,Source, Destination], axis = 1)
df1_train.drop(['Airline','Source','Destination'],axis = 1, inplace = True)

df1_train.rename(columns={'Air Asia':'Air_Asia','Jet Airways':'Jet_Airways','Air India':'Air_India','Jet Airways Business':'Jet_Airways_Business',
                            'Multiple carriers':'Multiple_carriers','Multiple carriers Premium economy':'Multiple_carriers_Premium_economy',
                            'Vistara Premium economy':'Vistara_Premium_economy'}, inplace =True)

#Working with the Test Data
df_test = pd.read_csv("F:\\flight\\flight243-main\\Test_set.csv")

df_test.isna().sum()

df_test['Day_of_travel']=pd.to_datetime(df_test["Date_of_Journey"],format = '%d/%m/%Y').dt.day
df_test['Month_of_travel']=pd.to_datetime(df_test["Date_of_Journey"],format = '%d/%m/%Y').dt.month


df_test.drop(["Date_of_Journey"],axis = 1, inplace = True)

# Extracting the departure time and min
df_test['Dep_hour']=pd.to_datetime(df_test['Dep_Time']).dt.hour
df_test['Dep_min']=pd.to_datetime(df_test['Dep_Time']).dt.minute

df_test.drop(['Dep_Time'], axis = 1, inplace = True)

# Extracting the arrival time and min
df_test['Arr_hour']=pd.to_datetime(df_test['Arrival_Time']).dt.hour
df_test['Arr_min']=pd.to_datetime(df_test['Arrival_Time']).dt.minute

df_test.drop(['Arrival_Time'], axis = 1, inplace = True)


# creating a list with the duration column
duration = list(df_test["Duration"])
# iterating the loop so that whichever line does have hrs/mins adding 0h or 0m at end/start acordingly.
for i in range(len(duration)):
    if len(duration[i].split()) != 2:
        if "h" in duration[i]:
            duration[i] = duration[i].strip() + " 0m"   ##removing unwanted spaces
        else:
            duration[i] = "0h " + duration[i]

# creating 2 columns and separating into hrs and mins columns.

duration_mins = []
duration_hours = []
for i in range(len(duration)):
    duration_hours.append(int(duration[i].split(sep = "h")[0]))
    duration_mins.append(int(duration[i].split(sep = "m")[0].split()[-1]))

''' separating the duration column into hours and mins column'''
df_test['Duration_mins'] = duration_mins

df_test.drop(['Duration'], axis = 1, inplace = True)

# One hot endcoding on the Airline data
Airline = df_test['Airline']
Airline = pd.get_dummies(Airline)

''' apply one hot encoding on destination column'''
Source = df_test[['Source']]
Source = pd.get_dummies(Source, drop_first = True)

''' apply one hot encoding on destination column'''
Destination = df_test[['Destination']]
Destination = pd.get_dummies(Destination, drop_first = True)

'''dropping the route column as its of no use and we have total stops to work out.'''
df_test.drop(['Route','Additional_Info'],axis = 1, inplace = True)

''' changing the values in the total_stops column into numbers'''
df_test['Total_Stops'].replace({'non-stop':0,'1 stop':1,'2 stops':2,'3 stops':3,'4 stops':4}, inplace =True)

''' creating the final train dataset'''
df1_test = pd.concat([df_train, Airline,Source, Destination], axis = 1)
df1_test.drop(['Airline','Source','Destination'],axis = 1, inplace = True)

df1_test.rename(columns={'Air Asia':'Air_Asia','Jet Airways':'Jet_Airways','Air India':'Air_India','Jet Airways Business':'Jet_Airways_Business',
                            'Multiple carriers':'Multiple_carriers','Multiple carriers Premium economy':'Multiple_carriers_Premium_economy',
                            'Vistara Premium economy':'Vistara_Premium_economy'}, inplace =True)

df1_train.columns

x = df1_train.loc[:,['Total_Stops', 'Day_of_travel', 'Month_of_travel', 'Dep_hour',
       'Dep_min','Air_Asia', 'Air_India', 'GoAir', 'IndiGo', 'Jet_Airways',
       'Jet_Airways_Business', 'Multiple_carriers',
       'Multiple_carriers_Premium_economy', 'SpiceJet', 'Trujet', 'Vistara',
       'Vistara_Premium_economy', 'Source_Chennai', 'Source_Delhi',
       'Source_Kolkata', 'Source_Mumbai', 'Destination_Cochin',
       'Destination_Delhi', 'Destination_Hyderabad', 'Destination_Kolkata',
       'Destination_New Delhi']]

y = df1_train.iloc[:,1]

'''Checking important feature using ExtaTree Regressor(Other ways are SelectKBest)'''
from sklearn.ensemble import ExtraTreesRegressor
selection = ExtraTreesRegressor()
selection.fit(x,y)

'''Fitting model using Random forest'''
from sklearn.model_selection import train_test_split
x_train, x_test, y_train, y_test = train_test_split(x,y,test_size =0.2, random_state = 42)

from sklearn.ensemble import RandomForestRegressor
reg_rf= RandomForestRegressor()
reg_rf.fit(x_train,y_train)

y_pred=reg_rf.predict(x_test)

'''training data score'''
reg_rf.score(x_train, y_train)

'''testing data score'''
reg_rf.score(x_test, y_test)

'''validating the metrics for regressor'''
from sklearn import metrics
logger.log_operation('INFO', format(metrics.mean_absolute_error(y_test,y_pred)))
logger.log_operation('INFO', format(metrics.mean_squared_error(y_test,y_pred)))
logger.log_operation('INFO', format(np.sqrt(metrics.mean_absolute_error(y_test,y_pred))))

#Hyper parameter tuning
''' we use either Randomized searchCV or Grid Search CV'''
from sklearn.model_selection import RandomizedSearchCV

#important parameters required

# number of trees in the random forest
n_estimators = [int(x) for x in np.linspace(start =100, stop = 1200, num=12)]
# number of features in consideration at every split
max_features = ['auto', 'sqrt']
# maximum number of levels allowed in each decision tree
max_depth = [int(x) for x in np.linspace(5, 50, num = 5)]
# minimum sample number to split a node
min_samples_split = [2, 6, 10]
# minimum sample number that can be stored in a leaf node
min_samples_leaf = [1, 3, 4]
# method used to sample data points
bootstrap = [True, False]


#creating the random grid
random_grid = {'n_estimators': n_estimators,'max_features': max_features,'max_depth': max_depth,
               'min_samples_split': min_samples_split,'min_samples_leaf': min_samples_leaf,
               'bootstrap': bootstrap}

#Random search of parameters using 5 fold cross validation
rf_random = RandomizedSearchCV(estimator = reg_rf,scoring = 'neg_mean_squared_error',  param_distributions = random_grid,n_iter = 10,
                               cv = 5, verbose=2,random_state=42, n_jobs = 1)

rf_random.fit(x_train,y_train)

rf_random.best_params_

prediction = rf_random.predict(x_test)

logger.log_operation('INFO',format(metrics.mean_absolute_error(y_test,y_pred)))
logger.log_operation('INFO',format(metrics.mean_squared_error(y_test,y_pred)))
logger.log_operation('INFO',format(np.sqrt(metrics.mean_absolute_error(y_test,y_pred))))

#pickling up the file
import pickle
file = open('flight.pkl','wb')
#dump file to the file

pickle.dump(rf_random,file)

model = open('flight.pkl','rb')
forest = pickle.load(model)

y_prediction= forest.predict(x_test)

metrics.r2_score(y_test,y_prediction)
