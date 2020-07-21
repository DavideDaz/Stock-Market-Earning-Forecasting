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


        dfMerged.to_csv(wd + '/MergedTables/' + t + '/{}_Fundamentals{}Merged.csv'.format(t,f.capitalize()))

        if f == 'quarterly':
            epsData = pd.read_csv(wd+'/epsHistorical/{}_eps_surprise.csv'.format(t))
            epsData.drop(epsData.columns[0], axis = 1, inplace = True)

            dfMerged['Date'] = dfMerged['Date'].astype('datetime64[ns]')
            epsData['Date'] = epsData['Date'].astype('datetime64[ns]')

            return dfMerged,epsData



    def mergeEPS(self,dfMerged,epsData,wd,t):

        dfMerged = dfMerged.sort_values(by='Date')
        epsData = epsData.sort_values(by='Date')

        dfMer = pd.merge_asof(dfMerged,epsData, on='Date', by='Symbol',direction='forward')

        dfMer = dfMer.sort_values(by='Date', ascending = False)

        dfMer.to_csv(wd + '/MergedTables/' + t + '/{}_FundamentalsMERGED.csv'.format(t))



    






        
        
