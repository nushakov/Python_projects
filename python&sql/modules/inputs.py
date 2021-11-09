import os
from datetime import datetime, timedelta


def _get_dates_interval(dates_int):
    '''#формирование промежутка дат'''
    try:
        dates_interval = []
        for _dates_int in dates_int.split(','):
            if len(_dates_int.split(':')) == 1:
                _dates_int = datetime.strptime(_dates_int, '%Y%m%d')#.date()
                dates_interval.append(_dates_int)
            else:
                start_date = datetime.strptime(str(_dates_int.split(':')[0]), '%Y%m%d')#.date()
                end_date = datetime.strptime(str(_dates_int.split(':')[1]), '%Y%m%d')#.date()
                dates_interval.append(start_date)
                if start_date != end_date:
                    while dates_interval[-1] != end_date:
                        dates_interval.append(dates_interval[-1] + timedelta(1))
                if len(_dates_int.split(':')) == 3:
                    _dates_interval = []
                    if _dates_int.split(':')[2] in ('w','W'):
                        for _date in dates_interval:
                            if _date.weekday() not in (5,6):
                                _dates_interval.append(_date)
                                dates_interval = _dates_interval
                    elif _dates_int.split(':')[2] in ('h','H'):
                        for _date in dates_interval:
                            if _date.weekday() in (5,6):
                                _dates_interval.append(_date)
                                dates_interval = _dates_interval
    except:
        raise Exception('\tОшибка!!! Недопустимый формат промежутка дат!\n')
        
    for _date in dates_interval:
        if _date > _date.today():
            raise Exception('\tОшибка!!! Дата должна быть не позже сегодняшней ' + str(_date.today()) + '!')
            
    return dates_interval
            
            
def _get_hours(hours):
    '''ввод временного интервала'''
    try:
        hour_start = int((hours.split('-')[0]))
        hour_end = int((hours.split('-')[1]))                
        if hour_start in range(0,4):
            hour_start += 24
        if hour_end in range(1,5):
            hour_end += 24
                                                
        if ((len(hours.split('-')) == 2) & (hour_start in range(4,28)) & 
            (hour_end in range(5,29)) & (hour_start < hour_end)):

            if hour_start >= 24:
                hour_start -= 24                    
            if hour_end >= 24:
                hour_end -= 24
        else:
            raise Exception('\tОшибка!!! Недопустимый формат временного интервала!\n')
    except:
        raise Exception('\tОшибка!!! Недопустимый формат временного интервала!\n')
        
    return [hour_start, hour_end]

    
def check_params(parameters):
    '''проверка введенных параметров'''
    #Даты БД UARMS:
    db_dates_interval = _get_dates_interval(parameters['db_dates'])
        
    #Временной интервал:
    hours = _get_hours(parameters['hours'])
        
    #Cценарий:
    scenario_id = int(parameters['scenario_id'])
        
    #Тип алгоритма:
    alg_type = str(parameters['alg_type'])
        
    #Тип расписания:
    schedule_type = str(parameters['schedule_type'])
    
    #Есть ли СВТ:
    is_hst = str(parameters['is_hst'])
            
    #Исключаемые трипы:
    drop_trips = parameters['drop_trips']
    if drop_trips != '':
        try:
            for _trip_id in drop_trips.split(','):
                int(_trip_id)
        except:
            raise Exception('\tОшибка!!! Недопустимый формат списка trip_id для исключения!\n')
            
    #Даты матрицы onoff
    if parameters['onoff_dates'] == 'f':
        onoff_dates_interval = 0
        if alg_type != 'прогнозный':
            raise Exception('\tОшибка!!! Выбор файла матрицы onoff возможен только для варианта алгоритма "прогнозный"!')
    else:
        onoff_dates_interval = _get_dates_interval(parameters['onoff_dates'])
    
    return onoff_dates_interval, db_dates_interval, hours, scenario_id, alg_type, schedule_type, is_hst, drop_trips


#Создание каталога по запросу
def ensure_dir(path):
    dir = os.path.dirname(path)
    if not os.path.exists(dir):
        os.makedirs(dir)
    return path
