import os 
import pandas as pd 
import time
import re 
import numpy as np 

directory = '' # directory of files you want to search through
input_directory = '' # directory of files with your search criteria
processed = '' # directory to store files with processed results
file_list = os.listdir(directory)
input_list = os.listdir(input_directory)
extracted_columns = [] # establish columns for extracted data files if needed


def filter_test(lookup, df):
    db = lookup[0] # establish current db name to search for
    table_name = lookup[1] # establish current table name to search for

    # establish various db and table name combinations to find in SQL column:
    sql_cond1 = fr'{db}.*{table_name}_'
    sql_cond2 = fr'{db}.*{table_name}\s'
    sql_cond3 = fr'{db}\s.*{table_name}\s'

    # use regex and dataframe.loc to check file for current search criteria. Be sure to add a line for each condition
    output = df.loc[df['SQL'].str.contains(sql_cond1, flags=re.I, regex=True, na=False) 
                    | [df['SQL'].str.contains(sql_cond1, flags=re.I, regex=True, na=False) 
                    | [df['SQL'].str.contains(sql_cond1, flags=re.I, regex=True, na=False)
                    , ['server', 'dbname', 'frequency']] # choose the columns to return
    output['table'] = np.nan
    output['table'] = output['table'].fillna(table_name)
    output['db'] = np.nan
    output['db'] = output['db'].fillna(db)
    output['dbname'] = output['dbname'].str.upper()
    output['server'] = output['server'].str.upper()
    server_name = df.iloc[0, 1]
    dbname = df.iloc[0, 2]
    if output.empty:
        print(f'did not find any matches for {db} and {table_name} \n')
        temp_df = pd.DataFrame([[server_name, dbname, db, table_name, 0]], columns=['server', 'dbname', 'db', 'table', 'frequency'])
        output = pd.concat([output, temp_df])
    print('Crawl complete, appending to Frequency dataframe')
    return output


def crawl(input_file):
    input_df = pd.read_csv(input_directory + input_file)
    input_columns = ['db', 'table', 'frequency']
    freq_df = pd.DataFrame(columns=input_columns)
    print(f'Parsing list {input_file} for criteria list')
    criteria_list = tuple(input_df.itertuples(index=False, name=None))
    print(f'Criteria list identified and stored\n')

    for raw in file_list:
        df = pd.read_csv(directory + raw, names=extracted_columns)
        raw_start = time.perf_counter() # create a timer to monitor how long all processes take for all search criteria on one raw data file
        for criteria in criteria_list:
            criteria_start = time.perf_counter() # create a timer to moniter how long all processes take for one search criteria on a data file
            print(f'Starting {criteria} crawl on {raw}')
            output = filter_test(criteria, df)
            print('Finished crawl')
            print(f'appending {criteria} to Frequency dataframe')
            freq_df = pd.concat([freq_df, output])
            print('Aggreagating Frequency dataframe and resetting index.')
            freq_df = freq_df.groupby(['server', 'dbname', 'db', 'table']).frequency.sum().reset_index()
            print(f'Aggregation complete. Displaying results:\n{freq_df}\n')
            criteria_end = time.perf_counter()
            print(f'Finished {criteria} crawl through {raw} in {round(criteria_end - criteria_start, 5)} seconds\n')
    output_name = re.sub('[.csv]', '', input_file) # create a name for results file based off of the input file name
    print(f'Writing frequency dataframe to file.\n')
    freq_df.to_csv(f'{processed}completed_{output_name}.csv')
    print(f'Frequency datafram saved as \"completed_{output_name}.csv\".\n')
    raw_finished = time.perf_counter()
    print(f'Crawl through {input_file} completed in {round(raw_finished - raw_start, 5)} seconds\n')
    return freq_df

start_time = time.perf_counter() # create a timer to monitor how long everything takes
list(map(crawl, input_list)) # run the crawl function to check all input files against all raw data files
end_time = time.perf_counter()
print(f'Total time taken: {round(end_time - start_time, 5)} seconds')
