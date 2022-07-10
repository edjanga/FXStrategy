import pandas as pd
import numpy as np
import sqlite3 as sql
import pdb
import datetime


class Signals(object):

    """
        Base class
    """

    def __init__(self,df):
        self.df = df
    @property
    def df(self):
        return  self.__df
    @df.setter
    def df(self,df):
        self.__df = df
    @staticmethod
    def weights(df,allocation='equally_weighted',m=None):
        pass

class FXSignals(Signals):

    """
        Class providing tools to generate trading signals based on standard trading FX strategies
    """

    def __init__(self,spot_df,fwd_df,vix_df):
        self.spot_df = spot_df
        self.fwd_df = fwd_df
        self.vix_df = vix_df

    @property
    def spot_df(self):
        return self.__spot_df
    @spot_df.setter
    def spot_df(self,spot_df):
        self.__spot_df = spot_df

    @property
    def fwd_df(self):
        return self.__fwd_df

    @fwd_df.setter
    def fwd_df(self,fwd_df):
        self.__fwd_df = fwd_df

    @property
    def vix_df(self):
        return self.__vix_df

    @vix_df.setter
    def vix_df(self, vix_df):
        self.__vix_df = vix_df

    def allocation_dd(self):
        return {'equally_weighted': {'adjusted': False, 'm': None}, \
                'momentum_m': {'adjusted': False, 'm': 5},
                'momentum_m_adjusted': {'adjusted': True, 'm': 5}}

    def carry(self,allocation):
        """
            Derive carry according to standard carry formula, i.e. log(spot_t/1M_fwd_(t+1))
            Adjusted: carry divided by the last 3 months of realised vol
        """
        adjusted_dd = self.allocation_dd()
        carry_df = self.spot_df.apply(np.log)-self.fwd_df.shift(-1).apply(np.log)
        if adjusted_dd[allocation]['adjusted']:
            realised_vol_3M_df = \
            self.spot_df.apply(lambda x:np.log(x)-np.log(x.shift())).rolling(window=3).std().shift(3)*np.sqrt(1/0.25)
            pdb.set_trace()
            carry_df = carry_df.div(realised_vol_3M_df)
        carry_df = carry_df.ffill()
        return carry_df

    def weights(self,allocation='equally_weighted'):
        """
            Note that JPY forward rates are negative --> inverted yield curve and carry NA
        """
        adjusted_dd = self.allocation_dd()
        if allocation == 'equally_weighted':
            weights_df = pd.DataFrame(columns=self.fwd_df.columns,index=self.fwd_df.index,data=1/self.fwd_df.shape[1])
        elif allocation in ['momentum_m','momentum_m_adjusted']:
            weights_df = pd.DataFrame(columns=self.fwd_df.columns,index=self.fwd_df.index)
            carry_df = self.carry(allocation)
            for idx, series_s in carry_df.iterrows():
                # Iteration through each date
                temp_s = series_s.dropna().sort_values(ascending=False)
                # Select top/bottom m currency yields
                long_s = pd.Series(data=1/adjusted_dd[allocation]['m'],\
                                   index=temp_s[:adjusted_dd[allocation]['m']].index)
                short_s = pd.Series(data=-1/adjusted_dd[allocation]['m'],\
                                    index=temp_s[-adjusted_dd[allocation]['m']:].index)
                signals_s = long_s.append(short_s)
                # Not selected currencies are set to 0 while keeping NAs --> survivor biais free
                no_position_idx = list(set(temp_s.index.tolist()).difference(set(signals_s.index.tolist())))
                no_position_s = pd.Series(index=no_position_idx, data=0.0)
                # Aggregated signals: long, short and no positions
                signals_s = signals_s.append(no_position_s)
                weights_df.loc[idx,signals_s.index] = signals_s
        return weights_df.shift()

    def ret(self,allocation):
        ret_df = self.weights(allocation).mul(self.carry(allocation))
        return ret_df

    def index(self,allocation):
        return self.ret(allocation).sum(axis=1)+1

    def all_strategies(self):
        strategies_ls = ['equally_weighted','momentum_m'] #,'momentum_m_adjusted'
        strategies_df = pd.DataFrame(index=self.fwd_df.index,columns=strategies_ls)
        for strategy in ['equally_weighted','momentum_m']: #,'momentum_m_adjusted'
            index_s = self.index(allocation=strategy)
            index_s.name = strategy
            strategies_df.loc[:,strategy] = index_s
        return strategies_df


if __name__ == '__main__':
    conn = sql.connect('../data/fx_pair.db')
    for name in ['spot','fwd','vix']:
        if name in ['spot','fwd']:
            query = f'SELECT t1.\"date\",t2.BRL,t2.TRY,t2.HUF,t2.CHF,t2.CAD,t2.AUD,\
                    t2.EUR,t2.NOK,t2.CZK,t2.PLN,t2.GBP,t2.KRW,t2.TWD,t2.JPY,t2.MXN,t2.SEK,t2.ZAR,t2.NZD,t2.SGD\
                    FROM rebalance AS t1\
                    INNER JOIN {name} AS t2\
                    ON t1.\"date\" = t2.\"date\";'
            if name == 'spot':
                spot_df = pd.read_sql(con=conn,sql=query,index_col='date')
            elif name == 'fwd':
                fwd_df = pd.read_sql(con=conn,sql=query, index_col='date')
        else:
            query = f'SELECT * FROM vix;'
            vix_df = pd.read_sql(con=conn,sql=query,index_col='date')
    fx_signals_obj = FXSignals(spot_df,fwd_df,vix_df)
    all_strategies_df = fx_signals_obj.all_strategies()
    print(f'[INSERTION]: Process has started @ {datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")}')
    all_strategies_df.to_sql(con=conn,if_exists='replace',name='strategies')
    name = 'strategies'
    print(f'[TABLE]: {name} has been inserted into the database.')
    print(f'[INSERTION]: Process completed @ {datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")}')
