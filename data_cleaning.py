from time import gmtime, strftime
import pandas as pd
import numpy as np

# Set start timer
s = strftime("%H:%M:%S", gmtime())
print(s)

# LOAD FILES
print('1/8 Load session data')
# countries = pd.read_csv('/Users/David/Documents/Programming/Springboard/CareerTrack/Projects/Airbnb/data/countries.csv')
# age_gender_bkts = pd.read_csv('/Users/David/Documents/Programming/Springboard/CareerTrack/Projects/Airbnb/data/age_gender_bkts.csv')
#sessions_og = pd.read_csv('/Users/David/Documents/Programming/Springboard/CareerTrack/Projects/Airbnb/data/sessions.csv', index_col = 0)
sessions_og = pd.read_csv('/Users/David/Documents/Programming/Springboard/CareerTrack/Projects/Airbnb/data/sessions.csv', index_col = 0, nrows=10000)

# LOAD FUNCTIONS
def list_of_lists_sums(list_of_lists):
    '''Takes in a list of equal lists and returns single list if values in lists > 0'''
    for indexX, i in enumerate(list_of_lists):
        for indexY, number in enumerate(i):
           if number > 0 :
               list_of_lists[indexX][indexY] = 1

    return_list = np.sum(list_of_lists, 0)
    return list(return_list)

def create_column_1_0(df, colnamelist):
    '''Creates a new binary column'''
    for colname in colnamelist:
        items = list(df['{}'.format(colname)]) #paste colname in
        new_items = [x if x == 0 else 1 for x in items] #if value is not zero make it a 1
        df['{}_0_1'.format(colname)] = new_items
    
    return df

# One hot encoding loop
import math

ids = set(list(sessions_og.index)) # create list of all ids to iterate through 
total_iterations = math.ceil(len(ids) / 100) # Creats toal number of iterations need for loop *no functionality
ids = [x for x in ids if str(x) != 'nan'] #remoce the nan in ids
ids = [ids[i:i+100] for i in range(0, len(ids), 100)] #makes id a lis

full_df = pd.DataFrame()
ticker = 0
print('2/8 One hot encoding loop')
for i in ids: 
    print('{}/{}'.format(ticker, total_iterations))
    ticker += 1 
    sessions = sessions_og[sessions_og.index.isin(i)]
    # CLEAN SESSION DATA 
    # ONE HOT #ENCODNING FOR SESSIONS AFTER DROPPING TIME
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

    # Create binary columns if a action/request/device is not 0
    colnames_to_convert = list(sessions_clnd)[6:156]  
    sessions_clnd = create_column_1_0(sessions_clnd, colnames_to_convert)
    
    full_df = full_df.append(sessions_clnd)
    
print('3/8 End of OHE')
# CREATE ADDTIONAL VARIBLES IN SESSION DATA 
# Create column with the number of login devices BETTER WAY??
devices = [list(full_df['device_type_Android Phone']),
    list(full_df['device_type_Mac Desktop']),
    list(full_df['device_type_Windows Desktop']),
    list(full_df['device_type_iPad Tablet']),
    list(full_df['device_type_iPhone'])]

full_df['devices_total'] = list_of_lists_sums(devices)

del devices

# CLEAN COLUMN NAMES
# replace spaces with _ in colnames
full_df.columns = full_df.columns.str.lower().str.replace(' ', '_')
 
# Reoder colnames ABC
full_df = full_df.reindex_axis(sorted(full_df.columns), axis=1)

# Export full_df to csv
print('4/8 Exporting full_df')
full_df.to_csv('/Users/David/Documents/Programming/Springboard/CareerTrack/Projects/Airbnb/data/sessions_clnd.csv')


# CLEAN TRAIN USERS
# Load
train_users_2 = pd.read_csv('/Users/David/Documents/Programming/Springboard/CareerTrack/Projects/Airbnb/data/train_users_2.csv', index_col = 0) 
# Create column if there was a destination made
destinations = train_users_2['country_destination']
made_reservation = [0 if x == 'NDF' else 1 for x in destinations]
train_users_2['made_reservation'] = made_reservation

# Create column for time from first active to first booking
train_users_2['date_account_created'] = pd.to_datetime(train_users_2['date_account_created'])
train_users_2['date_first_booking'] = pd.to_datetime(train_users_2['date_first_booking'])
train_users_2['elapsed_time'] = train_users_2['date_first_booking'] - train_users_2['date_account_created']

# JOING SESSION DATA WITH USER DATA
print('5/8 Joining tain on full_df')
test_users = pd.read_csv('/Users/David/Documents/Programming/Springboard/CareerTrack/Projects/Airbnb/data/test_users.csv', index_col = 0)
train = train_users_2.join(full_df)
print('6/8 Joining test on full_df')
test = test_users.join(full_df)

print('7/8 Writing train data')
train.to_csv('/Users/David/Documents/Programming/Springboard/CareerTrack/Projects/Airbnb/data/train.csv')
print('8/8 Writing test data')
test.to_csv('/Users/David/Documents/Programming/Springboard/CareerTrack/Projects/Airbnb/data/test.csv')

# Set end time and calc elapsed time 
e = strftime("%H:%M:%S", gmtime())
print('Start time: {}'.format(s))
print('End time: {}'.format(e))