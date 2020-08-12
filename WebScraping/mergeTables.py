import pandas as pd
from datetime import datetime
from math import nan
from functools import reduce


class tabMerge():

    def mergeTab(self,wd,symbolsTab,fundamentalsTab,genreMappings):
        symbols = symbolsTab['Symbol']
        symbols = ['AAPL']
        fundamentalsToMerge = fundamentalsTab['Mark']
        for s in symbols:
            # get quarter subdivision
            quarterMark = self.getQuarterSubdivision(wd,s)

            # merge all fundamentals in one table 
            dataFrames = self.mergeFundamentals(wd,s,fundamentalsToMerge,quarterMark)
            dfMerged = reduce(lambda  left,right: pd.merge(left,right,left_index=True,right_index=True,how='outer'), dataFrames)
            
            # aggregate eps surprise 
            epsSurprisedMerged = self.mergeEPS(dfMerged,wd,s)
            
            # clean Prices table and calculate averages
            prices = self.pricesSMAandEMA(s,wd)

            # create final table
            finalTable = self.mergePricesFundamentals(epsSurprisedMerged,prices)

            # add GICS sector column
            sectorN = symbolsTab.loc[symbolsTab['Symbol'] == s, 'GICS Sector'].iloc[0]
            finalTable['GICS Sector'] = sectorN

            finalTable.to_csv(wd + '/mergedTables'+ '/{}_merged.csv'.format(s))




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
        return dfMer


    def pricesSMAandEMA(self,symbol,wd):
        df = pd.read_csv(wd+'/../ScriptsSPprices/Stocks/{}_prices.csv'.format(symbol))
        df['ma90_Close'] = df['Close'].rolling(window=90).mean()
        df['ema_Close'] = df['Close'].ewm(com = 5,min_periods =90).mean()
        df['ma90_Volume'] = df['Volume'].rolling(window=90).mean()
        df['ema_Volume'] = df['Volume'].ewm(com = 5,min_periods =90).mean()
        df = df[['Date','Close','ma90_Close','ema_Close','ma90_Volume','ema_Volume']]
        return df

    def mergePricesFundamentals(self,fundamTable,pricesTable):
        pricesTable['Date'] = pricesTable['Date'].astype('datetime64[ns]')

        pricesTable.set_index('Date',inplace=True)

        mergedFundPrices = pd.merge_asof(fundamTable,pricesTable, left_index=True,right_index=True,direction='forward')

        mergedFundPrices['percent_return'] = mergedFundPrices['Close'].pct_change(periods=1)

        mergedFundPrices.drop('Close', axis = 1, inplace = True)

        mergedFundPrices.truncate(after = '2020-03-31')

        firstIndex = mergedFundPrices.notna().idxmax().max()

        mergedFundPrices = mergedFundPrices[firstIndex:].interpolate(limit_direction = 'forward').fillna(method='bfill')

        return mergedFundPrices

    






        
        
