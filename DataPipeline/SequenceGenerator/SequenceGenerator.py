import os
import pandas as pd 
import torch
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import LabelEncoder


class SequenceBuilder():
    def __init__(self, sequenceLength, targetLength, targetFeatures:list):
        self.dfFolderPath = 'DataPipeline/DataPreprocessing/MergedTables/'
        self.GICSsectorpath = 'DataPipeline/WebScrapingZacks/docs/Symbols.csv'
        self.sequenceLength = sequenceLength
        self.targets = targetFeatures
        self.numTargets = len(targetFeatures)
        self.targetLength = targetLength
        self.inputFeatures = None
        self.scaler = None
        self.symbolTable = pd.read_csv(self.GICSsectorpath)
        self.gicsMap,self.gicsFeaturesLength = self.__GICSMapping(self.symbolTable)

    def Split(self,train:list,validation:list,test:list):
        hashDfTrain, hashDfValidation, hashDfTest = {}, {}, {}
        for t in train:
            file = t+'_merged.csv'
            df = pd.read_csv(self.dfFolderPath+file,index_col=[0])
            hashDfTrain[t] = df
        for v in validation:
            file = v+'_merged.csv'
            df = pd.read_csv(self.dfFolderPath+file,index_col=[0])
            hashDfValidation[v] = df
        for s in test:
            file = s+'_merged.csv'
            df = pd.read_csv(self.dfFolderPath+file,index_col=[0])
            hashDfTest[s] = df


        self.__getLabels(hashDfTrain[list(hashDfTrain.keys())[0]]) # get features labels
        self.scaler = self.__computeStandardization(hashDfTrain) # compute scaler over all training data

        x_train_wf, y_train_wf = self.__multivariateDatasplitWrapper(hashDfTrain,0,self.scaler)
        x_validation_wf, y_validation_wf = self.__multivariateDatasplitWrapper(hashDfValidation,0,self.scaler)

        assert x_train_wf[0][-1].shape == torch.Size([self.sequenceLength, len(self.inputFeatures)]), 'Wrong Train Input shape'
        assert x_train_wf[1][-1].shape == torch.Size([self.gicsFeaturesLength]), 'Wrong Train Condition Input shape'
        assert y_train_wf[-1].shape == torch.Size([self.targetLength, self.numTargets]), 'Wrong Train Target shape'
        assert x_validation_wf[0][-1].shape == torch.Size([self.sequenceLength, len(self.inputFeatures)]), 'Wrong Validation Input shape'
        assert x_validation_wf[1][-1].shape == torch.Size([self.gicsFeaturesLength]), 'Wrong Validation Condition Input shape'
        assert y_validation_wf[-1].shape == torch.Size([self.targetLength, self.numTargets]), 'Wrong Validation Target shape'
        
        return x_train_wf, y_train_wf, x_validation_wf, y_validation_wf, self.scaler , hashDfTest

    def __computeStandardization(self,hashTrain):
        listDf = [hashTrain[k] for k in hashTrain.keys()]
        concatDf = pd.concat(listDf)
        assert self.inputFeatures != None, 'The columns labels order has not been defined'
        dataToNormalize = concatDf[self.inputFeatures]
        x = dataToNormalize.values #returns a numpy array
        scaler = StandardScaler()
        x_scaled = scaler.fit(x)
        return scaler
    
    def __multivariateDatasplitWrapper(self,elementToSplit,startIdx,scaler):
        if isinstance(elementToSplit,dict):
            inputData,targetData,conditionData = [],[],[]
            for k in elementToSplit.keys():
                inputDataK,targetDataK = self.multivariateDatasplit(elementToSplit[k],startIdx, 
                                                None, self.sequenceLength, self.targetLength, 1,scaler)
                inputData += inputDataK
                targetData += targetDataK
                conditionData += self.__GICSencoding(self.gicsMap,self.gicsFeaturesLength,k)*len(inputDataK)

        elif isinstance(elementToSplit,pd.DataFrame):
            inputData,targetData = self.multivariateDatasplit(elementToSplit[k],startIdx, 
                                                None, self.sequenceLength, self.targetLength, 1,scaler)
            conditionData += self.__GICSencoding(self.gicsMap,self.gicsFeaturesLength,k)*len(inputData)
        return [torch.tensor(inputData).reshape(-1,self.sequenceLength,len(self.inputFeatures)), \
                                                torch.tensor(conditionData).reshape(-1,self.gicsFeaturesLength).float()], \
                                                torch.tensor(targetData).reshape(-1,self.targetLength,self.numTargets)
    
    def multivariateDatasplit(self,df,startIndex, end_index, historySize, targetSize, step,scaler):
        data = []
        labels = []
        dataset = df[self.inputFeatures].values
        dfScaled = scaler.transform(dataset)
        dataset_target = dfScaled[:,0:self.numTargets]
        startIndex = startIndex + historySize
        if end_index is None:
            end_index = len(dataset) - targetSize
        for i in range(startIndex, end_index+1,targetSize):
            indices = range(i-historySize, i, step)
            data.append(dfScaled[indices])
            labels.append([dataset_target[i:i+targetSize]])
        return data, labels

    def __getLabels(self,df):
        columnLabels = df.columns.values
        self.inputFeatures = self.targets + [x for x in columnLabels if x not in self.targets]

    def __GICSMapping(self,df):
        gle = LabelEncoder()
        gics_labels = gle.fit_transform(df['GICS Sector'])
        gics_mappings = {label: index for index, label in 
                        enumerate(gle.classes_)}
        length = len(list(gics_mappings.keys()))
        return gics_mappings,length

    def __GICSencoding(self,mapper,length,name):
        ohe = [0]*length
        index = mapper[self.__getGICS(self.symbolTable,name)]
        ohe[index] = 1
        return ohe

    def __getGICS(self,df,name):
        gics = df.loc[df.Symbol == name, 'GICS Sector'].reset_index(drop=True)
        return gics.iloc[0]