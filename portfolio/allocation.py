import pandas as pd
import numpy as np
from scipy.optimize import minimize, LinearConstraint
import sqlite3 as sql
import pdb

class Weights:

    """
        Class providing several allocation techniques
    """

    def __init__(self):
        pass
    @staticmethod
    def equally_weighted(df):
        weights_df = pd.DataFrame(columns=df.columns,index=df.index,data=1/df.shape[1])
        return weights_df
    @staticmethod
    def momentum_ranking(df,qty=5):
        weights_df = pd.DataFrame(columns=df.columns,index=df.index)
        for idx, series_s in df.iterrows():
            # Iteration through each date
            temp_s = series_s.dropna().sort_values(ascending=False)
            # Select top/bottom m currency yields
            long_s = pd.Series(data=1/qty,index=temp_s[:qty].index)
            short_s = pd.Series(data=-1/qty,index=temp_s[-qty:].index)
            signals_s = long_s.append(short_s)
            # Not selected currencies are set to 0 while keeping NAs --> survivor biais free
            no_position_idx = list(set(temp_s.index.tolist()).difference(set(signals_s.index.tolist())))
            no_position_s = pd.Series(index=no_position_idx, data=0.0)
            # Aggregated signals: long, short and no positions
            signals_s = signals_s.append(no_position_s)
            weights_df.loc[idx, signals_s.index] = signals_s
        return weights_df.shift()
    def minimise_portfolio_variance_obj_func(self,weights,cov):
        return np.dot(np.transpose(weights),np.dot(cov,weights))
    @staticmethod
    def minimise_portfolio_variance(df):
        weights_ls = []
        A = [1]*df.shape[1]
        cov_mat = df.expanding().cov()
        for idx,series_s in df.iterrows():
            if(idx==df.index[0]):
                weights_ls.append([0]*df.shape[1])
            else:
                cov_mat = cov_mat.loc[:idx,:]#df.expanding().cov().loc[:idx,:]
                res_opt = minimize(fun=lambda x,cov_mat:np.dot(np.transpose(x),np.dot(cov_mat,x)),\
                                   x0=weights_ls[-1],args=(cov_mat),\
                                   constraints=LinearConstraint(A=A,lb=1,ub=1,keep_feasible=True))
                weights_ls.append(list(res_opt.x))
        weights_df = pd.DataFrame(data=weights_ls,index=df.index,columns=df.columns)
        return weights_df
    @staticmethod
    def maximise_carry(df,carry_df,annualised_vol=.10):
        """
            :param target_vol: annualised vol
        """
        t = 365//df.index.to_series().diff().min().days
        target_vol = annualised_vol/np.sqrt(t)
        weights_ls = []
        cov = df.expanding().cov()
        for idx,series_s in df.iterrows():
            if(idx==df.index[0]):
                print(idx)
                weights_ls.append([0]*df.shape[1])
            else:
                cov_mat = cov.loc[idx,:]
                res_opt = minimize(fun=lambda x:-np.dot(x,carry_df.loc[idx,:]),x0=weights_ls[-1],\
constraints=({'type':'ineq','fun':lambda x:np.dot(np.transpose(x),np.dot(cov_mat,x))-target_vol},\
             {'type':'ineq','fun':lambda x:-np.dot(np.transpose(x),np.dot(cov_mat,x))+target_vol}))
                print(list(res_opt.x))
                weights_ls.append(list(res_opt.x))
        weights_df = pd.DataFrame(data=weights_ls, index=df.index, columns=df.columns)
        pdb.set_trace()
        return weights_df



if __name__ == '__main__':
    conn = sql.connect('../data/fx_pair.db')
    for name in ['spot','fwd']:
        query = f'SELECT t1.\"date\",t2.BRL,t2.TRY,t2.HUF,t2.CHF,t2.CAD,t2.AUD,\
                            t2.EUR,t2.NOK,t2.CZK,t2.PLN,t2.GBP,t2.KRW,t2.TWD,t2.JPY,t2.MXN,t2.SEK,t2.ZAR,t2.NZD,t2.SGD\
                            FROM rebalance AS t1\
                            INNER JOIN {name} AS t2\
                            ON t1.\"date\" = t2.\"date\";'
        if name == 'spot':
            spot_df = pd.read_sql(con=conn, sql=query, index_col='date')
            spot_df.index = pd.to_datetime(spot_df.index)
        elif name == 'fwd':
            fwd_df = pd.read_sql(con=conn, sql=query, index_col='date')
    allocator = Weights()
    carry_df = spot_df.apply(np.log) - fwd_df.shift(-1).apply(np.log)
    carry_df = carry_df.ffill()
    #ret_df = spot_df.apply(lambda x:np.log(x)-np.log(x.shift()))
    allocation_df = allocator.maximise_carry(spot_df,carry_df)
    pdb.set_trace()




