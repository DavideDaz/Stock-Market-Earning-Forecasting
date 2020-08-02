import pandas as pd
from datetime import datetime
from math import nan
from functools import reduce


class tabMerge():
    def mergeTab(self,wd,symbols):
        files = pd.read_csv(wd+'/docs/FundamentalsList.csv')
        fundamentalsToMerge = files['Mark']
        for s in symbols:
            quarterMark = self.getQuarterSubdivision(wd,s)
            dataFrames = self.mergeFundamentals(wd,s,fundamentalsToMerge,quarterMark)
            dfMerged = reduce(lambda  left,right: pd.merge(left,right,left_index=True,right_index=True,how='outer'), dataFrames)
            self.mergeEPS(dfMerged,wd,s)


    def getQuarterSubdivision(self,wd,s):
        name = wd + '/FundamentalsHistorical/' + s + '/' + '{}_beta.csv'.format(s)
        df = pd.read_csv(name,index_col=[0])
        df['Date'] = df['Date'].astype('datetime64[ns]')
        quarterSubdivision = sorted(df['Date'].dt.month.unique().tolist())
        assert quarterSubdivision == [3,6,9,12] or quarterSubdivision == [1,4,7,10], 'Anomalous Quarter subdivision: {}'.format(quarterSubdivision)
        quarterMark = 'Q-DEC' if quarterSubdivision == [3, 6, 9, 12] else 'Q-JAN'
        return quarterMark

    def mergeFundamentals(self,wd,s,fundamentalsToMerge,quarterMark):
        dataFrames = []
        for i in fundamentalsToMerge:
                name = wd + '/FundamentalsHistorical/' + s + '/' + '{}_{}.csv'.format(s,i)
                df = pd.read_csv(name,index_col=[0])
                df['Date'] = df['Date'].astype('datetime64[ns]')
                dfr = df.resample(quarterMark, convention='end',on='Date').agg('mean')
                dataFrames.append(dfr)
        return dataFrames



    def mergeEPS(self,dfMerged,wd,s):

        name = wd + '/epsHistorical/' + '/' + '{}_eps_surprise.csv'.format(s)
        epsData = pd.read_csv(name,index_col=[0])
        epsData['Date'] = epsData['Date'].astype('datetime64[ns]')
        epsData.sort_values(by='Date', inplace=True)
        epsData.set_index('Date', inplace=True)
        epsData = epsData[['Symbol','EPS Surprise']]
        epsData.drop(labels='Symbol',axis=1,inplace=True)

        dfMer = pd.merge_asof(dfMerged,epsData, left_index=True,right_index=True,direction='forward')
        dfMer.to_csv(wd + '/MergedTables/' + s + '/{}.csv'.format(s))



    






        
        
