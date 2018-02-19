from time import gmtime, strftime
import pandas as pd
import numpy as np

# Set start timer
s = strftime("%H:%M:%S", gmtime())
print(s)

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



# ONE HOT ENCODING CHUNKS (44 min)
# Loads session data in chunks of 5000/10567737, one hot encoded each chunk and add binary column then writes to CSV
print('Load Data In Chunks of 100,000 to Create Dummies and Write to CSV, ~45 min')
print(strftime("%H:%M:%S", gmtime()))
ticker = 0 
for chunk in pd.read_csv('/Users/David/Documents/Programming/Springboard/CareerTrack/Projects/Airbnb/data/Original/sessions.csv',chunksize=100000, index_col = 0): 
    print('Chunk {}/106'.format(ticker))
    # Create new csv file to append to 
    pd.DataFrame().to_csv('/Users/David/Documents/Programming/Springboard/CareerTrack/Projects/Airbnb/data/Chunks/chunk_{}.csv'.format(ticker))

    # Drop time
    chunk_dummies = chunk.drop('secs_elapsed', 1)

    #Create dummies 
    chunk_dummies = pd.get_dummies(chunk_dummies,drop_first=True)

    # Loop through each id in sessions create empty df to append encoding to
    user_ids = set(chunk.index)
    chunk_ohe = pd.DataFrame()

    for i in user_ids:
     df = chunk_dummies.loc[chunk_dummies.index == i] #Filters by id
     values = df.sum(axis=0) #Sums values vertically
     row = values.to_frame().T #Trasnposed data
     row['id'] = i
     chunk_ohe = chunk_ohe.append(row) #Addes row to df

    # Reset index    
    chunk_ohe.set_index('id', inplace=True)

    # Create binary columns if a action/request/device is not 0
    colnames_to_convert = list(chunk_ohe)[6:156]
    chunk_ohe = create_column_1_0(chunk_ohe, colnames_to_convert)
    del colnames_to_convert

    # Write to apropriate chunk
    with open('/Users/David/Documents/Programming/Springboard/CareerTrack/Projects/Airbnb/data/Chunks/chunk_{}.csv'.format(ticker), 'a') as f:
     chunk_ohe.to_csv(f, header=True)

    ticker += 1



# UNIFY ALL DATA BY RELOADING CHUNKS 3 min
# Reload all data chunks and concat them together
print('Reload Data Chunks and Unify ~3 min')
print(strftime("%H:%M:%S", gmtime()))
import glob
path =r'/Users/David/Documents/Programming/Springboard/CareerTrack/Projects/Airbnb/data/Chunks'
allFiles = glob.glob(path + "/*.csv")

list_ = []
for i in allFiles:
 dfx = pd.read_csv(i,skiprows=[0])
 list_.append(dfx)

# Create Unified df
print('Unify Data')
sessions_clnd = pd.concat(list_, axis=0)

# Reset Index 
sessions_clnd.set_index('id', inplace=True)

# replace spaces with _ in colnames
sessions_clnd.columns = sessions_clnd.columns.str.lower().str.replace(' ', '_')

# Reoder colnames ABC
sessions_clnd = sessions_clnd.reindex_axis(sorted(sessions_clnd.columns), axis=1)

# write to file
sessions_clnd.to_csv('/Users/David/Documents/Programming/Springboard/CareerTrack/Projects/Airbnb/data/sessions_clnd.csv')



# CALC MIN MAX MEAN SESSION TIMES 34 min
print('Relaod Session Data and Calculate Session Time Info, ~34 min')
print(strftime("%H:%M:%S", gmtime()))
ticker = 0
ch_ticker = 0

for chunk in pd.read_csv('/Users/David/Documents/Programming/Springboard/CareerTrack/Projects/Airbnb/data/Original/sessions.csv',chunksize=100000, index_col = 0):
 print('{}/106'.format(ch_ticker))
 times = pd.DataFrame(chunk['secs_elapsed'])
 user_ids = set(times.index)

 for i in user_ids:
     # print('{}/{}'.format(ticker, len(user_ids)))
     if type(i) == str: 
         df = times.loc[times.index == i] #Filters by id

         user_times = list(df['secs_elapsed'])
         del df
         user_times = [x for x in user_times if str(x) != 'nan'] #remove nan

         if user_times != []:
             # print(np.std(user_times))
             row_min = min(user_times)

             row_max = max(user_times)
             row_mean = np.mean(user_times)
             row_sum = np.sum(user_times)
             if len(user_times) > 1:
                 row_std = np.std(user_times)
                 row_len = len(user_times)  
         else:
             row_min = 0
             row_max = 0
             row_mean = 0
             row_sum = 0
             row_std = 0
             row_len = 0
        
         row_id = i
        
         row = pd.DataFrame(data={'id': [row_id], 'session_time_min': [row_min], 
             'session_time_max': [row_max], 'session_time_mean': [row_mean], 
             'session_time_std': [row_std], 'session_time_total': [row_sum], 'session_count': [row_len]})  
         if ticker == 0:
             with open('/Users/David/Documents/Programming/Springboard/CareerTrack/Projects/Airbnb/data/times.csv', 'a') as f:
                 row.to_csv(f, header=True, index=True)
         else:
             with open('/Users/David/Documents/Programming/Springboard/CareerTrack/Projects/Airbnb/data/times.csv', 'a') as f:
                 row.to_csv(f, header=False, index=True)
         ticker += 1
 ch_ticker += 1



# MERGE TIME AND SESSION DATA TOGETHER
print('Unify Time and Cleaned Session Data ~3 min')
print(strftime("%H:%M:%S", gmtime()))

# Reload cleaned sessions
sessions_clnd = pd.read_csv('/Users/David/Documents/Programming/Springboard/CareerTrack/Projects/Airbnb/data/sessions_clnd.csv', index_col = 0)
sesh_times = pd.read_csv('/Users/David/Documents/Programming/Springboard/CareerTrack/Projects/Airbnb/data/times.csv', index_col = 0)

# Fix index
sesh_times.set_index('id', inplace=True)

# Combine dub rows
sesh_times = sesh_times.groupby(sesh_times.index).sum()
print(len(set(list(sesh_times.index))) == len(list(sesh_times.index)))

# Join sessions with time
full_session = sesh_times.join(sessions_clnd)
full_session = full_session.fillna(0)
full_session.to_csv('/Users/David/Documents/Programming/Springboard/CareerTrack/Projects/Airbnb/data/full_session.csv')



# CLEAN TRAIN USERS
print('Clean User Data >1 min')
print(strftime("%H:%M:%S", gmtime()))

# Load
train_users_2 = pd.read_csv('/Users/David/Documents/Programming/Springboard/CareerTrack/Projects/Airbnb/data/Original/train_users_2.csv', index_col = 0) 

# Create column if there was a destination made
destinations = train_users_2['country_destination']
made_reservation = [0 if x == 'NDF' else 1 for x in destinations]
train_users_2['made_reservation'] = made_reservation

# Create column for time from first active to first booking
train_users_2['date_account_created'] = pd.to_datetime(train_users_2['date_account_created'])
train_users_2['date_first_booking'] = pd.to_datetime(train_users_2['date_first_booking'])
train_users_2['elapsed_time'] = train_users_2['date_first_booking'] - train_users_2['date_account_created']



# JOING SESSION DATA WITH USER DATA
print('Join and Write Users ~5 min')
print(strftime("%H:%M:%S", gmtime()))
test_users = pd.read_csv('/Users/David/Documents/Programming/Springboard/CareerTrack/Projects/Airbnb/data/Original/test_users.csv', index_col = 0)
full_session = pd.read_csv('/Users/David/Documents/Programming/Springboard/CareerTrack/Projects/Airbnb/data/full_session.csv')
full_session.set_index('id', inplace=True)

train = pd.merge(train_users_2, full_session, left_index = True, right_index=True)
test = pd.merge(test_users, full_session, left_index = True, right_index=True)

train.to_csv('/Users/David/Documents/Programming/Springboard/CareerTrack/Projects/Airbnb/data/train.csv')
test.to_csv('/Users/David/Documents/Programming/Springboard/CareerTrack/Projects/Airbnb/data/test.csv')

# Set end time and calc elapsed time 
e = strftime("%H:%M:%S", gmtime())
print('Start time: {}'.format(s))
print('End time: {}'.format(e))