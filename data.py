import json
import sqlite3 as sql
import glob
import pandas as pd
import datetime




if __name__ == '__main__':

    with open('currency_info.json','r') as f:
        config = json.load(f)
    config = {key:value[0] for key,value in config.items()}
    config = pd.DataFrame(config).transpose()
    print(f'[INSERTION]: Process has started @ {datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")}')
    for kind in ['spot','fwdp']:
        data_df = pd.DataFrame()
        files_ls = glob.glob(f'data/*_{kind}.csv')
        for idx,file in enumerate(files_ls):
            print(f'[FILE] {idx}: Appending {file} to the master {kind} table.')
            temp_df = pd.read_csv(file)
            if(temp_df.shape[1]>2):
                # Ppresence of three columns in the fwdp file instead of two
                # --> Swap column order
                swap_columns_ls = temp_df.columns.tolist()
                dummy_swap = swap_columns_ls[1]
                swap_columns_ls[1] = swap_columns_ls[-1]
                swap_columns_ls[-1] = dummy_swap
                temp_df.columns = swap_columns_ls
                temp_df = temp_df[swap_columns_ls[:-1]]
            try:
                temp_df['Date'] = pd.to_datetime(temp_df['Date'],format='%d-%b-%y')
            except ValueError:
                try:
                    temp_df['Date'] = pd.to_datetime(temp_df['Date'], format='%d %B %Y')
                except ValueError as value_error:
                    with open('data/log.txt','a') as f:
                        f.write(value_error)
            temp_df = temp_df.set_index('Date')
            if data_df.empty:
                data_df = pd.concat([data_df,temp_df],axis=1)
            else:
                temp_join_df = data_df.copy()
                data_df = temp_join_df.join(temp_df,on='Date',how='left')
        data_df = data_df.sort_index()
        data_df = data_df.expanding().median()
        # Some currencies were introduced later in the dataset
        # Therefore, they have been left unprocessed dor dates prior their introduction
        data_df.columns = data_df.columns.str.replace('Line\(Q','').str.replace('=\)','').str.replace('1M','')
        data_df.index.name = None
        data_df = data_df.transpose()
        if kind == 'spot':
            data_df = data_df.join(config,how='left')
            currencies_ls = data_df.columns.tolist()
            currencies_ls.remove('style')
            currencies_ls.remove('fxMult')
            data_df.loc[data_df['style']=='European',currencies_ls] = \
            data_df.loc[data_df['style']=='European'].drop(['style','fxMult'],axis=1)**(-1)
            fxMult_s = data_df['fxMult']
            fxMult_s.name = None
            spot_df = data_df[currencies_ls]
            spot_df = spot_df.transpose()
        else:
            fwdp_df = data_df.copy()
            # Rename BRLNDF to BRL and ' KRW ' tp KRW
            fwdp_df = fwdp_df.transpose()
            fwdp_df = fwdp_df.rename(columns={'BRLNDF':'BRL',' KRW ':'KRW'})
            fwdp_df = fwdp_df.transpose()
            fwdp_df = fwdp_df.apply(lambda x: x * fxMult_s)
            fwdp_df = fwdp_df.transpose()
    # fwd = spot + fxMult * 1Mfwdp
    fwd_df = spot_df+fwdp_df
    conn = sql.connect('data/fx_pair.db')
    spot_df.to_sql(con=conn,if_exists='replace',name='spot')
    fwd_df.to_sql(con=conn,if_exists='replace', name='fwd')
    # Insertion of Rebalance dates and VIX
    rebalance_df = pd.read_csv('RebalanceDates.csv')
    vix_df = pd.read_csv('VIX.csv')
    container_dd = {'rebalance':rebalance_df,'vix':vix_df}
    for name,df in container_dd.items():
        df['Date'] = pd.to_datetime(df['Date'], format='%d %B %Y')
        df = df.set_index('Date')
        df.to_sql(con=conn,if_exists='replace',name=name)
        print(f'[FILE]: {name} has been inserted into the database.')
    conn.close()
    print(f'[INSERTION]: Process completed @ {datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")}')


