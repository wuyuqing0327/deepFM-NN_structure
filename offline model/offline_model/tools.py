import tensorflow as tf
from sklearn import metrics
import sys
import math
from sklearn.metrics import roc_auc_score
import numpy as np

# define optimzatin metric function
def auroc(y_true, y_pred):
    return tf.py_func(roc_auc_score, (y_true, y_pred), tf.double)

def ks(y_true, y_pred):
    fpr,tpr,thresholds= metrics.roc_curve(y_true,y_pred)
    return max(tpr-fpr)

def cal_ks(y_true, y_pred):
    return tf.py_func(ks, (y_true, y_pred), tf.double)

def fea_psi_calc(actual,predict,bins=10):
    '''
    功能: 计算连续变量和离散变量的PSI值
    输入值:
    actual: 一维数组或series，代表训练集中的变量
    predict: 一维数组或series，代表测试集中的变量
    bins: 违约率段划分个数
    输出值: 
    字典，键值关系为{'psi': PSI值，'psi_fig': 实际和预期占比分布曲线}
    '''
        
    psi_dict = {}
    psi_cut = []
    actual_bins = []
    predict_bins = []
    actual_len = len(actual)
    predict_len = len(predict)
    if actual.isnull().any() == True:
        bins = bins-1
        actual_cnt = actual.isna().sum()
        predict_cnt = predict.isna().sum()
        actual_pct = (actual_cnt+0.0) / actual_len
        predict_pct = (predict_cnt+0.0) / predict_len
        psi = (actual_pct-predict_pct) * math.log(actual_pct/predict_pct)
        psi_cut.append(psi)
    actual_temp = actual.dropna()
    predict_temp = predict.dropna()
    if len(actual_temp) > 0: 
        actual = np.sort(actual_temp)
        actual_distinct = np.sort(list(set(actual)))
        predict = np.sort(predict_temp)
        predict_distinct = np.sort(list(set(predict)))
        actual_distinct_len = len(actual_distinct)
        predict_distinct_len = len(predict_distinct)
        actual_min = actual.min()
        actual_max = actual.max()
        cuts = []
        binlen = (actual_max-actual_min) / bins
        if (actual_distinct_len<bins):
            for i in actual_distinct:
                cuts.append(i)
            for i in range(2, (actual_distinct_len+1)):
                if i == bins:
                    lowercut = cuts[i-2]
                    uppercut = float('Inf')
                else:
                    lowercut = cuts[i-2]
                    uppercut = cuts[i-1]
                actual_cnt = ((actual >= lowercut) & (actual < uppercut)).sum()+1
                predict_cnt = ((predict >= lowercut) & (predict < uppercut)).sum()+1
                actual_pct = (actual_cnt+0.0) / actual_len
                predict_pct = (predict_cnt+0.0) / predict_len
                psi_cut.append((actual_pct-predict_pct) * math.log(actual_pct/predict_pct))
                actual_bins.append(actual_pct)
                predict_bins.append(predict_pct)
        else:
            for i in range(1, bins):
                cuts.append(actual_min+i*binlen)
            for i in range(1, (bins+1)):
                if i == 1:
                    lowercut = float('-Inf')
                    uppercut = cuts[i-1]
                elif i == bins:
                    lowercut = cuts[i-2]
                    uppercut = float('Inf')
                else:
                    lowercut = cuts[i-2]
                    uppercut = cuts[i-1]
                actual_cnt = ((actual >= lowercut) & (actual < uppercut)).sum()+1
                predict_cnt = ((predict >= lowercut) & (predict < uppercut)).sum()+1
                actual_pct = (actual_cnt+0.0) / actual_len
                predict_pct = (predict_cnt+0.0) / predict_len
                psi_cut.append((actual_pct-predict_pct) * math.log(actual_pct/predict_pct))
                actual_bins.append(actual_pct)
                predict_bins.append(predict_pct)
    psi = sum(psi_cut)
    nbins = len(actual_bins)
    xlab = np.arange(1, nbins+1)
    psi_dict['psi'] = psi
    return psi_dict


def auc(y_score, y_test, thr=0.5):
    fpr, tpr, thresholds = metrics.roc_curve(y_test, y_score, pos_label = 1)
    return metrics.auc(fpr, tpr)
    
def ks(y_true, y_pred):
    fpr,tpr,thresholds= metrics.roc_curve(y_true,y_pred)
    return max(tpr-fpr)

