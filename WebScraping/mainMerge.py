import pandas as pd
import os
import sys

import mergeTables

if __name__ == "__main__":

    wd = os.getcwd()

    if not os.path.exists(wd + '/mergedTables'):
        os.mkdir(wd + '/mergedTables')

    tickersData = pd.read_csv(wd+'/docs/Symbols.csv')
    tickers = list(tickersData['Symbol'])

    mergeT = mergeTables.tabMerge()

    mergeT.mergeTab(wd,tickers)
