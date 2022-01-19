import pyantiML
import pandas as pd
import gc
from pathlib import Path
import multiprocessing
import numpy as np

# Function: Turns Full Transactions CSV into Yearly CSVs
def full_to_years_csv(full_pd):
    for year in range(min(full_pd['TIMESTAMP']).year,max(full_pd['TIMESTAMP']).year+1):
        year_pd = full_pd[full_pd['TIMESTAMP'].dt.year == year]
        full_pd[(full_pd['TIMESTAMP']>max(year_pd['TIMESTAMP'])) & (full_pd['TIMESTAMP']<=max(year_pd['TIMESTAMP'])+pd.Timedelta(days=7))]
        year_pd.to_csv(f'{year}.csv', sep = '|', index = False)
    return 1

# Function: Turns Yearly CSVs to Monthly CSVs and appends 7 days of data from the next month
def years_to_months_csv(csv_year_list):
    for i in range(0, len(csv_year_list)):
        csv_year_pd =  pd.read_csv(csv_year_list[i], sep = '|')
        csv_year_pd['TIMESTAMP'] = pd.to_datetime(csv_year_pd['TIMESTAMP'], infer_datetime_format=True, errors='coerce')

        for month in range(1,13):
            print(f'{csv_year_list[i]}file {month}month')
            month_pd = csv_year_pd[csv_year_pd['TIMESTAMP'].dt.month == month]

            if len(month_pd) == 0:
                print(f'{csv_year_list[i]} file {month} month has no length!')
                continue
            #max_val = max(month_pd['TIMESTAMP'])
            #print(f'Max Time stamp PD: {max_val}')
            
            month_pd = month_pd.append(csv_year_pd[(csv_year_pd['TIMESTAMP']>max(month_pd['TIMESTAMP']))&(csv_year_pd['TIMESTAMP']<=max(month_pd['TIMESTAMP'])+pd.Timedelta(days=7))])
            
            month_pd = month_pd[month_pd['RECEIVER'] != month_pd['SENDER']]
            month_pd.to_csv(''.join([str(csv_year_list[i])[0:4],'_',str(month),'.csv']), sep = '|', index = False)


            if (i == len(csv_year_list)-1) & (month == 12):

                month_pd = csv_year_pd[csv_year_pd['TIMESTAMP'].dt.month == 12]       
                month_pd = month_pd[month_pd['RECEIVER']!=month_pd['SENDER']]
                month_pd.to_csv(''.join([csv_year_list[i][0:4],'_',month,'.csv']), sep = '|', index = False)
                continue

            if month == 12:
                month_pd = csv_year_pd[csv_year_pd['TIMESTAMP'].dt.month == 12]
                month_pd = month_pd[month_pd['RECEIVER'] != month_pd['SENDER']]

                next_csv_year_pd =  pd.read_csv(csv_year_list[i+1], sep = '|')
                next_csv_year_pd['TIMESTAMP'] = pd.to_datetime(next_csv_year_pd['TIMESTAMP'], infer_datetime_format=True, errors='coerce')
                if len(month_pd) == 0:
                    continue
                month_pd = month_pd.append(next_csv_year_pd[(next_csv_year_pd['TIMESTAMP']>max(month_pd['TIMESTAMP']))&(next_csv_year_pd['TIMESTAMP']<=max(month_pd['TIMESTAMP'])+pd.Timedelta(days=7))])
                month_pd = month_pd[month_pd['RECEIVER'] != month_pd['SENDER']]
                month_pd.to_csv(''.join([str(csv_year_list[i])[0:4],'_',str(month),'.csv']), sep = '|', index = False)
    return 1


if __name__ == '__main__':
    
    #Executes Full to Yearly and cleans up memory
    full_pd = pd.read_csv('transactions_full.csv', sep = '|')
    full_pd['TIMESTAMP'] = pd.to_datetime(full_pd['TIMESTAMP'], infer_datetime_format=True, errors='coerce')
    full_to_years_csv(full_pd)
    del full_pd
    #del month_pd
    gc.collect()

    #Executes Yearly to Monthly
    path = Path('./')
    csv_year_list=sorted(path.rglob('20[0-2][0-9].csv'))
    years_to_months_csv(csv_year_list)

    #Delete Files:
    for f in path.rglob('20[0-2][0-9].csv'):
        f.unlink()
    with Path('./transactions_full.csv') as f:
        f.unlink()
        
    for itter_year in path.rglob('20[0-2][0-9]_*.csv'):
        print(f'Starting ML detection for year {str(itter_year)}')
        myantiml = pyantiML.antiML.from_csv(itter_year)
        gc.collect()
        pool = multiprocessing.Pool(12)
        random_ids = myantiml.unique_ids
        split_nparr_unique_ids = np.array_split(random_ids, 12)
        result = pool.map(myantiml.sus_bridges_pd,split_nparr_unique_ids)
        del myantiml
        gc.collect()
        pool.close()
        pool.join()
        print(result)
        itter_year.unlink()

    df_list = []
    for x in path.rglob('temp*.csv'):
        print(x)
        df_list.append(pd.read_csv(x, header = None))
        x.unlink()
    result_pd = pd.DataFrame({'FROM':pd.concat(df_list)[0],'FROM_TRANS':pd.concat(df_list)[1],'Suspected Bridge':pd.concat(df_list)[2],'TO_TRANS':pd.concat(df_list)[3],'TO':pd.concat(df_list)[4]})

    result_pd = result_pd.drop_duplicates()

    pd.concat([result_pd['FROM_TRANS'],result_pd['TO_TRANS']]).drop_duplicates().to_csv('suspicious_transactions.csv',index=False, header=False)

    final_sus_entities = pd.DataFrame(pd.concat([result_pd[['FROM_TRANS','Suspected Bridge']].drop_duplicates()['Suspected Bridge'].value_counts(),
    result_pd[['FROM','FROM_TRANS']].drop_duplicates()['FROM'].value_counts(),
    result_pd[['TO','TO_TRANS']].drop_duplicates()['TO'].value_counts()]).sort_values(ascending=False))

    final_sus_entities['Entities'] = final_sus_entities.index
    final_sus_entities['Entities'].to_csv('suspicious_entities.csv', header = False, index=False)