import pandas as pd
from datetime import datetime
from math import nan
from functools import reduce


class tabMerge():
    def mergeTab(self,f,wd,t):
        files = pd.read_csv(wd+'/docs/FundamentalsList.csv')
        fundamentalsToMerge = files.loc[files['TimeFrequency'] == f, 'Mark']


        dataFrames = []

        for i in fundamentalsToMerge:
            name = wd + '/FundamentalsHistorical/' + t + '/' + '{}_{}.csv'.format(t,i)
            df = pd.read_csv(name)
            df.drop(df.columns[0], axis = 1, inplace = True)
            dataFrames.append(df)

        dfMerged = reduce(lambda  left,right: pd.merge(left,right,on=['Date','Symbol'], how='outer'), dataFrames)
        dfMerged.drop_duplicates(inplace=True)

        epsData = pd.read_csv(wd+'/epsHistorical/{}_eps_surprise.csv'.format(t))

        dfMer = pd.merge_asof(dfMerged,epsData, on='Date', by='Symbol', tolerance=pd.Timedelta('32d'))

        dfMer.to_csv(wd + '/FundamentalsHistorical/' + t + '/{}_FundamentalsMERGED.csv'.format(t))



    






        
        
