import pandas as pd
import numpy as np

#Load File
countries = pd.read_csv('/Users/David/Documents/Programming/Springboard/CareerTrack/Projects/Airbnb/data/countries.csv')
age_gender_bkts = pd.read_csv('/Users/David/Documents/Programming/Springboard/CareerTrack/Projects/Airbnb/data/age_gender_bkts.csv')
test_users = pd.read_csv('/Users/David/Documents/Programming/Springboard/CareerTrack/Projects/Airbnb/data/test_users.csv', index_col = 0, nrows=1000)
train_users_2 = pd.read_csv('/Users/David/Documents/Programming/Springboard/CareerTrack/Projects/Airbnb/data/train_users_2.csv', index_col = 0, nrows=1000)
sessions = pd.read_csv('/Users/David/Documents/Programming/Springboard/CareerTrack/Projects/Airbnb/data/sessions.csv', index_col = 0, nrows=5000)


# LOAD FUNCTIONS
def list_of_lists_sums(list_of_lists):
    '''Takes in a list of equal lists and returns single list if values in lists > 0'''
    for indexX, i in enumerate(list_of_lists):
        for indexY, number in enumerate(i):
           if number > 0 :
               list_of_lists[indexX][indexY] = 1

    return_list = np.sum(list_of_lists, 0)
    return list(return_list)

#ONE HOT #ENCODNING FOR SESSIONS AFTER DROPPING TIME
# Drop time
sessions_dummies = sessions.drop('secs_elapsed', 1)

#Create dummies 
sessions_dummies = pd.get_dummies(sessions_dummies,drop_first=True)

# Loop through each id in sessions
user_ids = set(sessions.index)
sessions_ohe = pd.DataFrame()

for i in user_ids:
    df = sessions_dummies.loc[sessions_dummies.index == i] #Filters by id
    values = df.sum(axis=0) #Sums values vertically
    row = values.to_frame().T #Trasnposed data
    row['id'] = i
    sessions_ohe = sessions_ohe.append(row) #Addes row to df

del sessions_dummies
# Reset index
sessions_ohe.set_index('id', inplace=True)


#MIN MAX MEAN SESSION TIME 
times = pd.DataFrame(sessions['secs_elapsed'])
sesh_times = pd.DataFrame()

for i in user_ids:
    df = times.loc[times.index == i] #Filters by id
    user_times = df['secs_elapsed']
    row = pd.DataFrame([[min(user_times), max(user_times), 
                         np.mean(user_times), np.std(user_times)]],
                        columns=['session_time_min','session_time_max',
                                 'session_time_mean', 'session_time_std'])
    row['session_time_total'] = len(user_times)
    row['session_count'] = np.sum(user_times)
    row['id'] = i
    sesh_times = sesh_times.append(row) #Addes row to df

del times
# Reset index
sesh_times.set_index('id', inplace=True)


# JOIN ONE HOT ENCODING WITH TIME SESSION DATA
sessions_clnd = sesh_times.join(sessions_ohe)
del sesh_times
del sessions_ohe

# CREATE ADDTIONAL VARIBLES IN SESSION DATA 
# Create column with the number of login devices BETTER WAY??
devices = [list(sessions_clnd['device_type_Android Phone']),
	list(sessions_clnd['device_type_Mac Desktop']),
	list(sessions_clnd['device_type_Windows Desktop']),
	list(sessions_clnd['device_type_iPad Tablet']),
	list(sessions_clnd['device_type_iPhone'])]

sessions_clnd['devices_total'] = list_of_lists_sums(devices)
del devices

# Create binary columns if a requast was sent
