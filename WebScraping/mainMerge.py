import pandas as pd
import os
import sys

import mergeTables

if __name__ == "__main__":

    wd = os.getcwd()

    mergeT = mergeTables.tabMerge()

    mergeT.mergeTab('other',wd,'AAL')

    FundamentalsTable, EpsTable = mergeT.mergeTab('quarterly',wd,'AAL')

    MergedQuarterlyFundamentals = mergeT.mergeEPS(FundamentalsTable, EpsTable,wd,'AAL')
