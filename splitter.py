import numpy as np
from sklearn import preprocessing
from scipy.stats import bartlett
import itertools


import grabber as gr

"""
Функция добавляет в фрейм синтетический столбец из сумм нормализованных
критериев гомогенности, для достижения бизнесс требований:
- равное кол-во участиков в группах

Так же столбец служит для тестирования гомогенности относительно сегмента.
"""


def preproc_riders_for_split(tmp):
    for i in ['ltv', 'since_rent', 'age']:
        x = tmp[[i]].values.astype(float)  # returns a numpy array
        min_max_scaler = preprocessing.MinMaxScaler()
        x_scaled = min_max_scaler.fit_transform(x)
        col = i + '_norm'
        tmp[col] = x_scaled.astype(float)
    cols_to_float = ['is_ios', 'is_male', 'has_bb']
    for i in cols_to_float:
        tmp[i] = tmp[i].astype('float64')
    tmp['homo_t'] = tmp['is_ios'] + tmp['is_male'] + \
        tmp['ltv_norm'] + tmp['since_rent_norm'] + tmp['age_norm'] + \
        tmp['has_bb']
    df = tmp[['user_id', 'homo_t']]
    return df


"""
Функция разбивает фрейм на заданное кол-во подгрупп в каждом сегменте.
Тестирует гомогенность подсегментов относительно друг друга и сохраняет
csv с user_id соответвующего названия.
Если условие гомогенности нарушено - файлы не будут сохранены.
"""


def split_test_save_riders():
    df = gr.get_riders_seg()
    riders_chunks = {'SEG1': 2, 'SEG2': 8, 'SEG3': 2, 'SEG4': 2, 'SEG5': 10,
                     'SEG6': 8, 'SEG7': 2, 'SEG8': 2, 'SEG9': 2}

    for k, v in riders_chunks.items():
        print(k, v)
        tmp = df.copy(deep=True)
        tmp = tmp[tmp['segment'].str.contains(k)]
        tmp.drop(columns=['segment'], inplace=True)
        tmp = preproc_riders_for_split(tmp)
        tmp.set_index('user_id', inplace=True)
        shuffled = tmp.sample(frac=1, replace=False, weights=tmp.groupby(
            'homo_t')['homo_t'].transform('count'))
        result = np.array_split(shuffled, v)
        pvals = []
        for x, y in itertools.combinations(result, 2):
            T, p_value = bartlett(x['homo_t'], y['homo_t'])
            pvals.append(float(p_value))
        print(pvals)
        if all(i > 0.05 for i in pvals):
            print(f"All {v} chunks in {k} are in equal variance according to \
    Bartlett test with p > 0.05")
            print('Saving chunks...')
            names = [k + '_' + str(n) + '.csv' for n in range(1, v+1)]
            for part, name in zip(result, names):
                part.drop(columns=['homo_t'], inplace=True)
                part.to_csv(name, encoding='utf-8')
        else:
            print('Barlett test has failed, reshuffle manually!')
            break


split_test_save_riders()
