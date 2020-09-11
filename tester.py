import abyes as ab
import numpy as np
import pandas as pd

import logging

logging.basicConfig(filename="bayes.log", level=logging.INFO,
                    format='%(message)s')


def check(data):
    exp = ab.AbExp(alpha=0.95, method='analytic', rule='rope',
                   decision_var='lift', plot=False)
    logging.info("")
    return logging.info("{}".format(exp.experiment(data)))


def split():
    df = pd.read_csv('test_7d.csv')
    camps = df.campaign_name.unique().tolist()
    segs = ['CONTROL', 'TEST']
    names = ['A', 'B']
    for c in camps:
        logging.info("")
        logging.info('Эксперимент в сегменте:')
        logging.info(c)
        tmp = df.copy(deep=True)
        tmp = tmp[tmp['campaign_name'] == c]
        data = []
        for s, n in zip(segs, names):
            logging.info("")
            seg = tmp.copy(deep=True)
            seg = seg[seg['segment_type'] == s]
            arr = seg.has_rides.to_numpy()
            riders = np.count_nonzero(arr == 1)
            ln = len(arr)
            conv = (riders/ln * 100)
            logging.info(f'{s} ({n}) размер группы {ln}')
            logging.info("конверсия %{:.4f}".format(conv))
            logging.info("")
            data.append(arr)
        check(data)


split()
