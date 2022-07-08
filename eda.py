import pandas as pd
import datetime
from statsmodels.tsa.ar_model import AutoReg
from statsmodels.stats.stattools import jarque_bera,durbin_watson
from statsmodels.tsa.stattools import adfuller
import sqlite3 as sql


class EDA(object):
    """
        Class used to proceed to explanatory data analysis
    """
    def __init__(self,df):

        #to_period('D') needed to fit AR(1) for tests using residuals below
        try:
            df.index = pd.to_datetime(df.index).to_period('D')
        except TypeError:
            #Extra processing needed for index (fwd)
            df = df.to_timestamp('D')
            df.index = pd.to_datetime(df.index).to_period('D')
        self.df = df

    @property
    def df(self):
        return self.__df
    @df.setter
    def df(self,df):
        self.__df = df
    def summary_statistics(self):
        """
        :return: DataFrame with the following statistics:
                 - Mean
                 - Median
                 - Std
                 - Max
                 - Min
                 - Skewness
                 - Kurtosis
                 - Mode
                 - First valid index
                 - Last valid index
                 - #NA
        """

        summary_df = self.df.agg([lambda x:x.mean(),\
                                  lambda x:x.median(),\
                                  lambda x:x.std(),\
                                  lambda x:x.max(),\
                                  lambda x:x.min(),\
                                  lambda x:x.skew(),\
                                  lambda x:x.kurtosis(),\
                                  lambda x:x.first_valid_index(),\
                                  lambda x:x.last_valid_index(),\
                                  lambda x:x.isnull().sum()]).transpose()
        summary_df.columns = ['mean','median','std','max','min','skew','kurtosis',\
                              'first_valid_index','last_valid_index','qtyNA']
        mode_df = self.df.apply(pd.Series.mode).transpose().rename(columns={0:'mode'})
        summary_df = summary_df.join(mode_df,how='inner')
        return summary_df
    @staticmethod
    def residuals_ar(series_s):
        """
        :return: residuals from AR(1) model run on series_s
        """
        model_obj = AutoReg(series_s,lags=1)
        model = model_obj.fit()
        return model.resid
    def tests(self):

        """
        :return: DataFrame with p-values to the following tests:
                 - Jarque Bera: H0 normality
                 - Durbin Watson: H0 autocorralation 0 <= DW <= 4
                          positive | DW = 2 ==> no correlation | negative
                 - ADF: H0 non-stationary
        """

        test_df = self.df.agg([lambda x:jarque_bera(x[x.first_valid_index():])[1],\
                               lambda x:durbin_watson(EDA.residuals_ar(x[x.first_valid_index():])),\
                               lambda x:adfuller(x[x.first_valid_index():])[1]
                               ])
        test_df = test_df.transpose()
        test_df.columns = ['jarque_bera','durbin_watson','adf']
        test_df = test_df.round(4)
        return test_df
    def overall_stats(self):
        """
        :return: DataFrame combining summary_statistics and tests
        """
        summary_statistics_df = self.summary_statistics()
        test_df = self.tests()
        overall_stats_df = summary_statistics_df.join(test_df,how='inner')
        #Data conversion to accommdate for insertion to db
        overall_stats_df = overall_stats_df.astype(str)
        return overall_stats_df


if __name__ == '__main__':
    conn = sql.connect('data/fx_pair.db')
    spot_df = pd.read_sql(con=conn,sql="SELECT * FROM spot;",index_col='index')
    fwd_df = pd.read_sql(con=conn, sql="SELECT * FROM fwd;",index_col='index')
    container_dd = {'spot': spot_df, 'fwd': fwd_df}
    print(f'[INSERTION]: Process has started @ {datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")}')
    for name,df in container_dd.items():
        eda_obj = EDA(spot_df)
        overall_stats_df = eda_obj.overall_stats()
        name = '_'.join((name,'eda'))
        overall_stats_df.to_sql(con=conn,if_exists='replace',name=name)
        print(f'[TABLE]: {name} has been inserted into the database.')
    print(f'[INSERTION]: Process completed @ {datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")}')
    conn.close()