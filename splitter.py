import grabber as gr
import numpy as np
import pandas as pd
from sklearn import preprocessing
from scipy.stats import levene
import os

pd.options.display.float_format = "{:.8f}".format

"""
Функция добавляет в фрейм синтетический столбец из сумм нормализованных
критериев гомогенности, для разбиения сегментов на равное кол-во
участиков в группах. Так же столбец служит для тестирования гомогенности.
"""


def add_synth_test_col(tmp):
    print(tmp.head(10))
    if 'ltv' in tmp.columns:
        for i in ['ltv', 'since_rent', 'age']:
            x = tmp[[i]].values.astype(float)  # returns a numpy array
            min_max_scaler = preprocessing.MinMaxScaler()
            x_scaled = min_max_scaler.fit_transform(x)
            col = i + '_norm'
            tmp[col] = x_scaled.astype(float)
            tmp.drop(columns=[i], inplace=True)
    else:
        print('Here')
        for i in ['age', 'dsa']:
            x = tmp[[i]].values.astype(float)  # returns a numpy array
            min_max_scaler = preprocessing.MinMaxScaler()
            x_scaled = min_max_scaler.fit_transform(x)
            col = i + '_norm'
            tmp[col] = x_scaled.astype(float)
            tmp.drop(columns=[i], inplace=True)
    tmp['homo_t'] = tmp[list(tmp.columns)].sum(axis=1)
    df = tmp[['homo_t']]
    return df


"""
Функция разбивает фрейм на заданное кол-во подгрупп в каждом сегменте n раз
до получения статистически не различимых вариантов в подгруппах
"""


def shuffle_test(tmp, v):
    n = 1
    while True:
        print(f"Iteration number {n}")
        rws = len(tmp)//v
        result = []
        for i in range(v-1):
            i = tmp.sample(n=rws, replace=False)
            tmp.drop(i.index, inplace=True)
            result.append(i)
        result.append(tmp)
        test_arr = [x['homo_t'].values for x in result]
        stat, p = levene(*test_arr)
        pyval = p.item()
        print('p-value due to Levene test:', '{0:4f}'.format(pyval))
        n += 1
        if pyval > 0.05:
            return result
            break
        elif n > 500:
            break


"""
Функция итерируется по df согласно списку сегментов, для каждого эл-та списка
создает гомогенные подгруппы и сохраняет их в csv.
"""


def split_save(df, chunks):

    for k, v in chunks.items():
        tmp = df.copy(deep=True)
        tmp = tmp[tmp['segment'].str.contains(k)]
        tmp.drop(columns=['segment'], inplace=True)
        tmp = add_synth_test_col(tmp)
        print(f"Sampling {v} chunks in {k}")
        result = shuffle_test(tmp, v)
        if result:
            print(f"All {v} chunks in {k} are in equal variance according to \
    Levene test")
            print('Saving chunks...')
            names = [k + '_' + str(n) + '.csv' for n in range(1, v+1)]
            for part, name in zip(result, names):
                part.drop(columns=['homo_t'], inplace=True)
                outdir = './segments'
                if not os.path.exists(outdir):
                    os.mkdir(outdir)
                part.to_csv(os.path.join(outdir, name), encoding='utf-8')
        else:
            print('Stop shuffling...')


if __name__ == "__main__":
    df = gr.get_riders_seg()
    chunks = {'SEG1': 2, 'SEG2': 8, 'SEG3': 2, 'SEG4': 2, 'SEG5': 10,
              'SEG6': 8, 'SEG7': 2, 'SEG8': 2, 'SEG9': 2}
    split_save(df, chunks)
    df = gr.get_dormants_seg()
    chunks = {'SEG10': 2, 'SEG11': 12, 'SEG12': 12}
    split_save(df, chunks)
