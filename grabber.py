# -*- coding: utf-8 -*-

from google.cloud import bigquery
import pandas as pd
import numpy as np
from pathlib import Path

import logging
from google.oauth2 import service_account

# загружаем данные для анализа
# df = get_df_from_bq(sql2)
# df.to_csv('test_df.csv', encoding='utf-8', index=False)

logging.basicConfig(filename="segments.log", level=logging.INFO,
                    format='%(message)s')


def get_df_from_bq(QUERY):
    """
    Функция обращается к BQ и возвращает df. Для запуска на локально машине
    необходимо прописать в конфиге баш (zshrc etc) переменную:
    $GOOGLE_APPLICATION_CREDENTIALS с абсолютным путем до json
    c auth2 BQ
    """
    credentials = service_account.Credentials()
    client = bigquery.Client(credentials=credentials,
                             project='carsharing-analytics')
    return client.query(QUERY).to_dataframe()


def process_frame(df):
    """
    Функция обрабатывает исходных df:
    - удалет строки с NULL
    - приводит платформу, пол и наличие доступа к BelkaBlack в бинарный вид
    """
    cnt = np.count_nonzero(df.isnull().values)
    df.dropna(inplace=True)
    df.drop_duplicates(subset='user_id', keep='first', inplace=True)
    print(f'Removed rows with NULL:{cnt}')
    df['is_ios'] = (df['platform'] == 'ios').astype(int)
    df['is_male'] = (df['gender'] == 'm').astype(int)
    df.drop(columns=['platform', 'gender'], inplace=True)
    df['has_bb'] = df['has_bb'].astype(int)
    df.set_index('user_id', inplace=True)
    return df


def add_riders_segment(df):
    """
    Функция добавляет в исходный df совершавших ренты колонку с наименованием
    сегмента согласно бизнесс логике
    """
    df['segment'] = np.nan
    df.loc[((df['ltv'] >= 7500) &
            (df['since_rent'].between(15, 89))), 'segment'] = 'SEG1'
    df.loc[((df['ltv'] >= 7500) &
            (df['since_rent'].between(90, 364))), 'segment'] = 'SEG2'
    df.loc[((df['ltv'] >= 7500) &
            (df['since_rent'] >= 365)), 'segment'] = 'SEG3'
    df.loc[((df['ltv'].between(2000, 7499)) &
            (df['since_rent'].between(15, 89))), 'segment'] = 'SEG4'
    df.loc[((df['ltv'].between(2000, 7499)) &
            (df['since_rent'].between(90, 364))), 'segment'] = 'SEG5'
    df.loc[((df['ltv'].between(2000, 7499)) &
            (df['since_rent'] >= 365)), 'segment'] = 'SEG6'
    df.loc[((df['ltv'] < 2000) &
            (df['since_rent'].between(15, 89))), 'segment'] = 'SEG7'
    df.loc[((df['ltv'] < 2000) &
            (df['since_rent'].between(90, 364))), 'segment'] = 'SEG8'
    df.loc[((df['ltv'] < 2000) &
            (df['since_rent'] >= 365)), 'segment'] = 'SEG9'
    return df


def add_dormants_segment(df):
    """
    Функция добавляет в исходный df не совершавших ренты но одобренных
    пользователей колонку с наименованием сегмента согласно бизнесс логике
    """
    df['segment'] = np.nan
    df.loc[(df['dsa'] < 90), 'segment'] = 'SEG10'
    df.loc[((df['dsa'].between(90, 364))), 'segment'] = 'SEG11'
    df.loc[(df['dsa'] >= 365), 'segment'] = 'SEG12'
    return df


def get_riders_seg():
    """
    Функция отдаем финальный df с сегментами по ездевшим пользователям
    """
    QUERY = (
        """SELECT
            user_id,
            total_revenue_rub as ltv,
            days_after_last_personal_nonzero_rent as since_rent,
            has_access_to_belkablack as has_bb,
            platform,
            age,
            gender
          FROM
            `carsharing-analytics.bi.user`
          WHERE
            total_personal_nonzero_rents > 0
            AND user_id IS NOT NULL
            AND verification_status IN ('approve_to_bb',
              'approve_to_bc')
            AND days_after_last_personal_nonzero_rent > 14"""
    )
    file = 'riders.csv'
    if Path(file).is_file():
        df = pd.read_csv(file, encoding='utf-8')
        df.set_index('user_id', inplace=True)
        print("File exist")
    else:
        print("File not exist, downloading...")
        df = get_df_from_bq(QUERY)
        df = add_riders_segment(df)
        df = process_frame(df)
        df.to_csv(file, encoding='utf-8')

    print(df.info())
    print(df.groupby(['segment']).size())
    logging.info("{}".format(df.info()))
    logging.info("{}".format(df.groupby(['segment']).size()))
    return df


def get_dormants_seg():
    """
    Функция отдаем финальный df с сегментами по не ездевшим но одобренным
    пользователям
    """
    QUERY = (
        """WITH
          sleepy AS (
          SELECT
            user_id,
            has_access_to_belkablack as has_bb,
            platform,
            age,
            gender,
            DATE_DIFF(CURRENT_DATE("UTC+3"),
            DATE(approve_status_first_started_at, "UTC+3"), DAY) AS dsa
          FROM
            `carsharing-analytics.bi.user`
          WHERE
            total_personal_nonzero_rents = 0
            AND user_id IS NOT NULL
            AND verification_status IN ('approve_to_bb',
              'approve_to_bc')
            AND approve_status_first_started_at IS NOT NULL
          GROUP BY
            user_id,
            approve_status_first_started_at,
            has_access_to_belkablack,
            platform,
            age,
            gender)
        SELECT
          *
        FROM
          sleepy
        WHERE
          dsa > 14"""
    )
    file = 'dormants.csv'
    if Path(file).is_file():
        df = pd.read_csv(file, encoding='utf-8')
        df.set_index('user_id', inplace=True)
        print("File exist")
    else:
        print("File not exist, downloading...")
        df = get_df_from_bq(QUERY)
        df = add_dormants_segment(df)
        df = process_frame(df)
        df.to_csv(file, encoding='utf-8')
    print()
    print(df.groupby(['segment']).size())
    logging.info("{}".format(df.info()))
    logging.info("{}".format(df.groupby(['segment']).size()))
    return df


if __name__ == "__main__":
    get_riders_seg()
    get_dormants_seg()
