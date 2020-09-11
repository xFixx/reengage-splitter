# -*- coding: utf-8 -*-

import grabber as gr
import pandas as pd
from scipy.stats import levene
import os


pd.options.display.float_format = "{:.8f}".format


def shuffle_test(df, v):
    """
    Функция разбивает фрейм на заданное кол-во подгрупп в каждом сегменте n раз
    до получения статистически не различимых вариантов в подгруппах
    """
    n = 1
    while True:
        tmp = df.copy(deep=True)
        print(f"Iteration number {n}")
        print(f'Total segment size {len(tmp)}')
        rws = len(tmp)//v
        result = []
        for i in range(v-1):
            i = tmp.sample(n=rws, replace=False)
            tmp.drop(i.index, inplace=True)
            result.append(i)
        print(f'Total last seg for control size {len(tmp)}')
        result.append(tmp)
        cn = tmp.copy(deep=True)
        p_arr = []
        for h in tmp.columns:
            test = [x[h].to_numpy() for x in result]
            stat, p = levene(*test)
            pyval = p.item()
            print(f'p-value due to Levene test for {h}:',
                  '{0:4f}'.format(pyval))
            p_arr.append(pyval)
        n += 1
        if all(i > 0.05 for i in p_arr):
            print(f'Total last seg for control size split {len(tmp)}')
            cs = len(cn)//v-1
            cntrl = []
            for c in range(v-2):
                c = cn.sample(n=cs, replace=False)
                cn.drop(c.index, inplace=True)
                cntrl.append(c)
            print(f'Last control size {len(tmp)}')
            cntrl.append(cn)
            print(f'В общем {len(result)}, в контрольной {len(cntrl)}')
            return result, cntrl
        elif n > 500:
            return None
            break


def split_save(df, chunks):
    """
    Функция итерируется по df согласно списку сегментов
    и сохраняет полученные подгруппы в csv.
    """
    for k, v in chunks.items():
        tmp = df.copy(deep=True)
        tmp = tmp[tmp['segment'].str.contains(k)]
        tmp.drop(columns=['segment'], inplace=True)
        print(f"Sampling {v} chunks in {k}")
        result, cntrl = shuffle_test(tmp, v)
        if result:
            print(f"All {v} chunks in {k} are in equal variance according to \
Levene test")
            print('Saving chunks...')
            names = [k + '_' + str(n) + '.csv' for n in range(1, v+1)]
            for part, name in zip(result, names):
                part.reset_index(level=0, inplace=True)
                this = part['user_id']
                outdir = './segments'
                if not os.path.exists(outdir):
                    os.mkdir(outdir)
                print(name, len(this))
                this.to_csv(os.path.join(outdir, name), encoding='utf-8',
                            index=False)
            # cntrl_n = [k + '_' + str(n) +'_control.csv' for n in range(1, v)]
            # for part, name in zip(cntrl, cntrl_n):
            #     part.reset_index(level=0, inplace=True)
            #     that = part['user_id']
            #     outdir = './segments'
            #     if not os.path.exists(outdir):
            #         os.mkdir(outdir)
            #     print(name, len(that))
            #     that.to_csv(os.path.join(outdir, name), encoding='utf-8')
        else:
            print('Cant split segments correctly, script stopped.')


if __name__ == "__main__":
    """
    Для каждого сегмента необходимо указать кол-во тестовых подгрупп
    + 1 контроль. Сегменты разбиваются на равные части гомогенно
    простой случайной выборкой.
    """
    df = gr.get_riders_seg()
    chunks = {'SEG1': 13, 'SEG2': 6, 'SEG3': 2, 'SEG4': 12, 'SEG5': 9,
              'SEG6': 2, 'SEG7': 9, 'SEG8': 8, 'SEG9': 3}
    split_save(df, chunks)
    df = gr.get_dormants_seg()
    chunks = {'SEG10': 2, 'SEG11': 4, 'SEG12': 7}
    split_save(df, chunks)
