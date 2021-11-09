from datetime import timedelta
from datasets.processed import askp_passflows, schedule
import tkinter
from tkinter.filedialog import askopenfilename

import pandas as pd


def get_fact_schedule(date, hours):
    '''данные о фактическом расписании'''
    try:
        df_fact = schedule.get(date,'fact')
    except:
        raise Exception('\tОшибка!!! Не выполнен вход в файлохранилище, либо за дату ' + str(date) + ' отсутствуют данные по фактическому расписанию!')
        
    _hours = []
    for i in range(2):
        _hour = pd.Timestamp(year = int(date.year), month = int(date.month),
                             day = int(date.day), hour = int(hours[i]))
        if hours[i] in range(0,4):
            _hour += timedelta(1)
        _hours.append(_hour)

    df_fact = df_fact[(df_fact.arrival_time >= _hours[0]) & 
                      (df_fact.arrival_time < _hours[1]) &
                      (df_fact.stop_sequence == 1)]
    df_fact = df_fact.sort_values(by = ['route_id','trip_id','arrival_time'])
    df_fact = df_fact.drop_duplicates(subset=['route_id','trip_id','grafic','trip_num','arrival_time'])
    df_fact['num_of_trips'] = df_fact.groupby(by = ['route_id','trip_id'])['stop_sequence'].transform(sum)
    df_fact = df_fact[['route_id','trip_id','num_of_trips']].drop_duplicates()
    
    return df_fact


def get_onoff(date, hours):
    '''данные матрицы onoffmatrix'''
    try:
        df_onoff = askp_passflows.get(date, 'onoffmatrix')
    except:
        raise Exception('\tОшибка!!! Не выполнен вход в файлохранилище, либо за дату ' + str(date) + ' отсутствуют данные по onoffmatrix!')
    
    _hours = []
    for i in range(2):
        _hour = pd.Timestamp(year = int(date.year), month = int(date.month),
                             day = int(date.day), hour = int(hours[i]))
        if hours[i] in range(0,4):
            _hour += timedelta(1)
        _hours.append(_hour)
            
    df_onoff = df_onoff[df_onoff.timestamp_on >= _hours[0]]
    if hours[1] != 4:                    
        df_onoff = df_onoff[df_onoff.timestamp_on < _hours[1]]
        
    df_onoff = df_onoff.drop(['timestamp_on','mode_id'], axis=1)
        
    return df_onoff
    
    
def get_onoff_from_file():
    '''прогнозная матрица из csv.файла'''
    while True:
        try:
            print('\tВыберете csv-файл прогнозной матрицы. Файл должен содержать поля "site_id_on","site_id_off" и "cnt"!')
            root = tkinter.Tk()
            root.wm_withdraw()
            load_file = askopenfilename()
            if load_file == '':
                _is_continue = str(input('\tФайл не выбран! Хотите продолжить?[y/n] '))
                while _is_continue not in ('Y','y','N','n'):
                    _is_continue = str(input('\tФайл не выбран! Хотите продолжить?[y/n] '))
                if _is_continue in ('N','n'):
                    print('\tЗавершение работы программы!')
                    root.destroy()
                    break
                    raise SystemExit
            else:
                file_name = load_file.split('/')[-1]
                df_onoff = pd.read_csv(load_file, encoding='cp1251', sep=r'[;,\s]')
                root.destroy()
                return df_onoff, file_name
        except:
            print('\tНеверный формат файла! Выберете другой файл!')