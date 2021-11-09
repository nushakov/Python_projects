import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm

import config

from modules.db_data import get_routes, get_stops, get_trip_stops, get_num_of_trips, get_capacity, get_capacities_by_types
from modules.datasets_data import get_fact_schedule, get_onoff, get_onoff_from_file
from modules.inputs import ensure_dir


#====== Выгрузка и усреднение данных из датасетов ======
def get_dsets_tables(onoff_dates_interval, db_dates_interval, hours, schedule_type):
    '''Усредненные данные датасетов'''
    #Фактическое расписание, только если выбран тип расписания - фактическое
    df_fact = pd.DataFrame(columns = ['route_id','trip_id'])
    if schedule_type == 'фактическое':
        print('\n\tФормируем таблицу фактического расписания')
        for _date in tqdm(db_dates_interval):
            _df_fact = get_fact_schedule(_date, hours)
            #Добавляем в общую таблицу кол-во рейсов за каждую дату
            _df_fact = _df_fact.rename(columns={'num_of_trips':'num_of_trips_' + str(_date).replace('-','')})
            df_fact = pd.merge(df_fact, _df_fact, how = 'outer', on = ['route_id','trip_id'])
        #Получаем среднее значение кол-ва рейсов для каждого трипа
        df_fact = df_fact.fillna(0)
        df_fact['num_of_trips'] = df_fact[df_fact.columns[2:]].mean(axis=1)
        df_fact = df_fact[['route_id','trip_id','num_of_trips']]
    
    if onoff_dates_interval == 0:
        df_onoff, file_name = get_onoff_from_file()
    else:
        df_onoff = pd.DataFrame(columns = ['route_id', 'trip_id_on', 'stop_sequence_on',
                                           'stop_id_on', 'trip_id_off', 'stop_sequence_off', 
                                           'stop_id_off', 'cnt'])
        file_name = ''
        print('\n\tФормируем таблицу матрицы onoffmatrix')
        for _date in tqdm(db_dates_interval):
            _df_onoff = get_onoff(_date, hours)
            df_onoff = pd.merge(df_onoff, _df_onoff, how='outer')
        try:
            df_onoff['cnt'] = df_onoff.groupby(by = ['route_id', 'trip_id_on', 'stop_sequence_on',
                                                     'stop_id_on', 'trip_id_off', 'stop_sequence_off', 
                                                     'stop_id_off'])['cnt'].transform(sum)
        except:
            df_onoff['cnt'] = df_onoff.groupby(by = ['route_id', 'stop_id_on', 'stop_id_off'])['cnt'].transform(sum)
    
    return df_fact, df_onoff, file_name

    
#====== Выгрузка и усреднение данных БД UARMS ======
def get_db_tables(db_dates_interval, hours, scenario_id, schedule_type, is_hst, drop_trips):
    '''Усредненные данные из базы'''
    #Усредненные за даты таблицы БД
    df_routes = pd.DataFrame(columns = ['mvn','route_id','trip_id','transport_type'])
    df_stops = pd.DataFrame(columns = ['stop_id','site_id','stop_lat','stop_lon','log_id'])
    df_trip_stops = pd.DataFrame(columns = ['route_id','trip_id','stop_sequence',
                                            'stop_id','cum_distance','log_id'])
    df_num_of_trips = pd.DataFrame(columns = ['route_id','trip_id'])
    df_capacity = pd.DataFrame(columns = ['route_id','trip_id','transport_type'])

    #Даты состояния БД (см. ниже)
    db_dates = []
    
    print('\n\tФормируем таблицы БД UARMS')
    for _date in tqdm(db_dates_interval):
        #Дата состояния БД - для каждой даты из списка - ближайшее воскресенье от самой поздней даты в интервале либо текущая дата, если она раньше
        _db_date = _date + timedelta(min(((_date.today() - _date).days),(timedelta(6 - _date.weekday()).days)))
        
        if _db_date not in db_dates:
            #Таблицы маршрутов, остановок и поостановочных трасс берем только за даты состояния БД и мерджим:
            _df_routes = get_routes(_db_date, scenario_id, is_hst, drop_trips)
            df_routes = pd.merge(df_routes, _df_routes, how='outer')
            
            _df_stops = get_stops(_db_date, scenario_id)
            df_stops = pd.merge(df_stops, _df_stops, how='outer')
            
            _df_trip_stops = get_trip_stops(_db_date, scenario_id)
            df_trip_stops = pd.merge(df_trip_stops, _df_trip_stops, how='outer')
            
            db_dates.append(_db_date)
            
        #Таблицы кол-ва т/с и вместимостей берем за каждую дату из перечня, т.к. в них учитывается таблица services, где есть конкретные дни недели
        if schedule_type == 'плановое':
            #Таблицу df_num_of_trips формируем, только если выбран тип расписания - плановое
            _df_num_of_trips = get_num_of_trips(_db_date, _date, scenario_id, hours, False)
            #Добавляем в общую таблицу кол-во рейсов за каждую дату
            _df_num_of_trips = _df_num_of_trips.rename(columns={'num_of_trips':'num_of_trips_' + str(_date).replace('-','')})
            _df_num_of_trips = _df_num_of_trips.drop_duplicates(subset=['route_id','trip_id'])
            _df_num_of_trips = _df_num_of_trips.drop(['log_id'], axis=1)
            df_num_of_trips = pd.merge(df_num_of_trips, _df_num_of_trips, how='outer', on = ['route_id','trip_id'])
            
        _df_capacity = get_capacity(_db_date, _date, scenario_id)
        #Добавляем в общую таблицу вместимость за каждую дату
        _df_capacity = _df_capacity.rename(columns={'capacity':'capacity_' + str(_date).replace('-','')})
        _df_capacity = _df_capacity.drop_duplicates(subset=['route_id','trip_id'])
        df_capacity = pd.merge(df_capacity, _df_capacity, how='outer', on = ['route_id','trip_id','transport_type'])
        
    #Приводим в окончательный вид полученные таблицы: оставляем только маршруты из df_routes, убираем дублирование и пр.
    df_stops = df_stops.sort_values(by = ['stop_id','log_id'])
    df_stops = df_stops.drop_duplicates(subset='stop_id', keep='last')
    df_stops = df_stops.drop(['log_id'], axis=1)
    
    df_trip_stops = pd.merge(df_trip_stops, df_routes[['route_id','trip_id']], how='right', on=['route_id','trip_id'])
    df_trip_stops = df_trip_stops.sort_values(by = ['route_id','trip_id','stop_sequence','log_id'])
    df_trip_stops = df_trip_stops.drop_duplicates(subset=['route_id','trip_id','stop_sequence'], keep='last')
    df_trip_stops = df_trip_stops.drop(['log_id'], axis=1)
    
    if schedule_type == 'плановое':
        #В таблицу num_of_trips добавляем усредненное по датам кол-во рейсов, и для каждого маршрута оставляем максимальное значение
        df_num_of_trips['num_of_trips'] = df_num_of_trips[df_num_of_trips.columns[2:]].mean(axis=1)
        df_num_of_trips = df_num_of_trips[['route_id','trip_id','num_of_trips']]
        df_num_of_trips = pd.merge(df_num_of_trips, df_routes[['route_id','trip_id']], how='right', on=['route_id','trip_id'])
        df_num_of_trips = df_num_of_trips.fillna(0)
        df_num_of_trips = df_num_of_trips.sort_values(by = ['route_id','trip_id','num_of_trips'])
        df_num_of_trips = df_num_of_trips.drop_duplicates(subset=['route_id','trip_id'], keep='last')
    
    #Аналогично по вместимостям
    df_capacity['capacity'] = df_capacity[df_capacity.columns[3:]].mean(axis=1)
    df_capacity = df_capacity[['route_id','trip_id','transport_type','capacity']]
    df_capacity = pd.merge(df_capacity, df_routes[['route_id','trip_id','transport_type']], 
                           how='right', on=['route_id','trip_id','transport_type'])
    df_capacity = df_capacity.fillna(0)
    #Для маршрутов с отсутствующими или нулевыми вместимостями, подтягиваем их из усредненных таблиц по видам транспорта
    df_capacities_ngpt, df_capacities_hst = get_capacities_by_types()
    df_capacities_by_types = pd.merge(df_capacities_ngpt, df_capacities_hst, how = 'outer')
    df_no_capacity = df_capacity[df_capacity.capacity == 0][['route_id','trip_id','transport_type']]
    df_no_capacity = pd.merge(df_no_capacity, df_capacities_by_types, how='inner', on='transport_type')
    df_capacity = pd.merge(df_capacity, df_no_capacity, how='outer')
    df_capacity = df_capacity.sort_values(by = ['route_id','trip_id','capacity'])
    df_capacity = df_capacity.drop_duplicates(subset=['route_id','trip_id'], keep='last')
    df_capacity = df_capacity.drop(['transport_type'], axis=1)

    return df_routes, df_stops, df_trip_stops, df_num_of_trips, df_capacity
    

#====== Подтягивание дополнительных num_of_trips ======
def get_additional_num_ot(df_num_of_trips, df_onoff, db_dates_interval, scenario_id, hours):
    '''попытка подтянуть num_of_trips'''
    #Маршруты, для которых num_of_trips = 0
    df_miss_routes = df_num_of_trips[df_num_of_trips.num_of_trips == 0][['route_id','trip_id']]
    #В исходной матрицы onoff для ранних дат (примерно ранее 02-03.2020) отсутствуют данные о trip_id
    try:
        df_miss_routes = pd.merge(df_miss_routes, 
                                  pd.merge(df_onoff[['route_id','trip_id_on']].drop_duplicates().rename(columns={'trip_id_on':'trip_id'}),
                                           df_onoff[['route_id','trip_id_off']].drop_duplicates().rename(columns={'trip_id_off':'trip_id'}),
                                           how = 'outer'), 
                                  how = 'inner')
    except ValueError:
        df_miss_routes = pd.merge(df_miss_routes, df_onoff[['route_id']].drop_duplicates(), how = 'inner', on = 'route_id')
    except KeyError:
        pass

        
    if len(df_miss_routes) > 0:
        print('\n\tВ матрице onoff присутствуют маршруты, для которых в БД не нашлось кол-во рейсов!!!')
    
        #Выгрузка из БД num_of_trips несколько иным способом
        df_additional_num_ot = pd.DataFrame(columns = ['route_id','trip_id'])

        #Даты состояния БД (см. ниже)
        db_dates = []
    
        print('\n\tПробуем выгрузить дополнительные кол-ва рейсов для пропущенных маршрутов')
        for _date in tqdm(db_dates_interval):
            #Дата состояния БД - для каждой даты из списка - ближайшее воскресенье от самой поздней даты в интервале либо текущая дата, если она раньше
            _db_date = _date + timedelta(min(((_date.today() - _date).days),(timedelta(6 - _date.weekday()).days)))
        
            if _db_date not in db_dates:
                #Таблицы маршрутов, остановок и поостановочных трасс берем только за даты состояния БД и мерджим:
                _df_additional_num_ot = get_num_of_trips(_db_date, _date, scenario_id, hours, True)
                #Добавляем в общую таблицу кол-во рейсов за каждую дату
                _df_additional_num_ot = _df_additional_num_ot.rename(columns={'num_of_trips':'num_of_trips_' + str(_date).replace('-','')})
                _df_additional_num_ot = _df_additional_num_ot.drop_duplicates(subset=['route_id','trip_id'])
                _df_additional_num_ot = _df_additional_num_ot.drop(['log_id'], axis=1)
                df_additional_num_ot = pd.merge(df_additional_num_ot, _df_additional_num_ot, how='outer', on = ['route_id','trip_id'])
            
        #В таблицу num_of_trips добавляем усредненное по датам кол-во рейсов, и для каждого маршрута оставляем максимальное значение
        df_additional_num_ot['num_of_trips'] = df_additional_num_ot[df_additional_num_ot.columns[2:]].mean(axis=1)
        df_additional_num_ot = df_additional_num_ot[['route_id','trip_id','num_of_trips']]
        df_additional_num_ot = pd.merge(df_additional_num_ot, df_miss_routes, how='right', on=['route_id','trip_id'])
        df_additional_num_ot = df_additional_num_ot.fillna(0)
        df_additional_num_ot = df_additional_num_ot.sort_values(by = ['route_id','trip_id','num_of_trips'])
        df_additional_num_ot = df_additional_num_ot.drop_duplicates(subset=['route_id','trip_id'], keep='last')
    
        df_additional_true = df_additional_num_ot[df_additional_num_ot.num_of_trips > 0]
        df_additional_false = df_additional_num_ot[~(df_additional_num_ot.num_of_trips > 0)][['route_id','trip_id']]
        
        if len(df_additional_true) > 0:
            #Добавляем дополнительные num_of_trips в основную таблицу:
            df_num_of_trips = pd.merge(df_num_of_trips, df_additional_true, how = 'outer')
            df_num_of_trips = df_num_of_trips.sort_values(by = ['route_id','trip_id','num_of_trips'])
            df_num_of_trips = df_num_of_trips.drop_duplicates(subset=['route_id','trip_id'], keep='last')
            
        if len(df_additional_false) > 0:
            #Маршруты для которых и по 2ой итерации не нашлось кол-во рейсов
            print('\t\tИнформация по маршрутам без кол-ва рейсов будет сохранена в итоговом файле во вкладке "routes_no_schedule"')
            
        return df_num_of_trips, df_additional_false
        
    else:
        return df_num_of_trips, df_miss_routes
        
        
#====== Формирование возможных связей ======
def get_spreads(df_routes, df_trip_stops, df_stops, df_num_of_trips, df_onoff, alg_type):
    '''формирование связей и поостановочных перегонов'''
    #добавляем сайты к поостановочным трассам
    df_trip_stops = pd.merge(df_trip_stops, df_stops[['stop_id','site_id']], how='left', on='stop_id')
    df_links = pd.merge(df_trip_stops, df_trip_stops, how='inner', on=['route_id','trip_id'])
    df_links = df_links.rename(columns={'stop_sequence_x':'stop_sequence_on', 
                                        'stop_id_x':'stop_id_on',
                                        'site_id_x':'site_id_on',
                                        'stop_sequence_y':'stop_sequence_off', 
                                        'stop_id_y':'stop_id_off',
                                        'site_id_y':'site_id_off'})
    #формируем все доступные связи по каждому trip_id:
    df_links = df_links[df_links.stop_sequence_off > df_links.stop_sequence_on]
    
    df_links = df_links.sort_values(by = ['route_id','trip_id','stop_sequence_on','stop_sequence_off'])
    
    #длина конкретной связи для каждого конкретного трипа:
    df_links['link_length'] = abs(df_links.cum_distance_y - df_links.cum_distance_x)
    df_links = df_links.drop(['cum_distance_x','cum_distance_y'], axis=1)
    
    #добавляем сайты к onoffmatrix, если матрица не из файла
    try:
        df_onoff = pd.merge(df_onoff, df_stops[['stop_id','site_id']].rename(columns={'stop_id':'stop_id_on',
                                                                                      'site_id':'site_id_on'}), 
                            how='left', on='stop_id_on')
        df_onoff = pd.merge(df_onoff, df_stops[['stop_id','site_id']].rename(columns={'stop_id':'stop_id_off',
                                                                                      'site_id':'site_id_off'}), 
                            how='left', on='stop_id_off')
    except:
        pass
                        
    #для удобства пронумеруем поездки в матрице onoff
    df_onoff['move_id'] = df_onoff.index
    

    #======= 1-ая итерация распределения поездок =======
    if alg_type == 'прогнозный':
        df_spreads = pd.merge(df_links, df_onoff[['site_id_on','site_id_off','cnt','move_id']], 
                             how = 'left', on = ['site_id_on','site_id_off'])
    else:
        try:
            #если в матрице onoff есть данные о trip_id
            df_spreads = pd.merge(df_links, df_onoff.rename(columns={'trip_id_on':'trip_id'}), how = 'left', 
                                  on = ['route_id','trip_id','stop_sequence_on','stop_id_on','site_id_on',
                                        'stop_sequence_off','stop_id_off','site_id_off'])
            df_spreads = df_spreads.drop(['trip_id_off'], axis=1)
        except:
            #если в матрице onoff нет данных о trip_id
            df_spreads = pd.merge(df_links, df_onoff[['route_id','stop_id_on','site_id_on','stop_id_off',
                                                      'site_id_off','cnt','move_id']],
                                  how = 'left', on = ['route_id','stop_id_on','site_id_on',
                                                      'stop_id_off','site_id_off'])

        #нераспределенные поездки пробуем распределить только среди сайтов
        df_onoff_no_trips = df_onoff[
                                (~(df_onoff.move_id.isin(df_spreads[df_spreads.move_id.notnull()].move_id.unique())))]
        df_spreads_sites = pd.merge(df_links, df_onoff_no_trips[['route_id','site_id_on','site_id_off','cnt','move_id']],
                                    how='left', on=['route_id','site_id_on','site_id_off'])
        df_spreads_sites = df_spreads_sites[df_spreads_sites.move_id.notnull()]
        df_spreads = df_spreads.append(df_spreads_sites)

    df_spreads = df_spreads.sort_values(by=['route_id', 'trip_id', 'stop_sequence_on', 'stop_sequence_off'])
    df_spreads['cnt'] = df_spreads.cnt.fillna(0)

    # исключаем дублирование распределения поездок внутри trip_id
    df_spreads_false = df_spreads[df_spreads.move_id.isnull()]

    df_spreads_true = df_spreads[df_spreads.move_id.notnull()]
    df_spreads_true['stop_seq_diff'] = df_spreads_true['stop_sequence_off'] - df_spreads_true['stop_sequence_on']
    df_spreads_true = df_spreads_true.sort_values(by=['route_id','trip_id','move_id','stop_seq_diff'])
    df_spreads_true = df_spreads_true.drop(['stop_seq_diff'], axis=1)
    df_spreads_true = df_spreads_true.reset_index()
    df_spreads_true = df_spreads_true.drop(['index'], axis=1)
    df_spreads_true['_index'] = df_spreads_true.index
    df_spreads_true_real = df_spreads_true.drop_duplicates(subset=['route_id','trip_id','move_id'])
    df_spreads_true = df_spreads_true[~(df_spreads_true['_index'].isin(df_spreads_true_real['_index'].unique()))]

    df_spreads_true['cnt'] = 0
    df_spreads_true = df_spreads_true.drop(['move_id','_index'], axis=1)
    df_spreads_true_real = df_spreads_true_real.drop(['_index'], axis=1)
    #добавляем move_id = null
    df_spreads_true = pd.merge(df_spreads_true, df_spreads_false[['trip_id','move_id']].drop_duplicates(),
                              how='left', on='trip_id')
    df_spreads_true = df_spreads_true.append(df_spreads_true_real)
    df_spreads_true = df_spreads_true.sort_values(by=['route_id','trip_id','stop_sequence_on','stop_sequence_off','cnt'])
    df_spreads_true = df_spreads_true.drop_duplicates()

    df_spreads = pd.merge(df_spreads_false, df_spreads_true, how='outer')
    df_spreads = df_spreads.sort_values(by=['route_id', 'trip_id', 'stop_sequence_on', 'stop_sequence_off'])

    #добавляем кол-во рейсов
    df_spreads = pd.merge(df_spreads, df_num_of_trips, how = 'left', on = ['route_id','trip_id'])

    #доля суммарного кол-ва рейсов реализующих связь для заданного trip_id
    if alg_type == 'прогнозный':
        df_spreads['part_num_of_trips'] = df_spreads.num_of_trips/df_spreads.groupby(by = ['move_id'])['num_of_trips'].transform(sum)
    else:
        #для связей реализуемых несколькими мвнами в рамках одного route_id
        df_spreads['part_num_of_trips'] = df_spreads.num_of_trips/df_spreads.groupby(by = ['route_id',
                                                         'move_id'])['num_of_trips'].transform(sum)
    #поправка на кол-во пассажиров:
    df_spreads['corr_cnt'] = df_spreads.cnt * df_spreads.part_num_of_trips
    df_spreads = df_spreads.drop(['part_num_of_trips'], axis=1)
    
    #ищем поездки (move_id) для которых нашлись только trip_id с нулевым кол-вом рейсов
    df_spreads['sum_num_ot'] = df_spreads.groupby(by = ['move_id'])['num_of_trips'].transform(sum)
    
    #нераспределенные по маршрутам поездки из матрицы onoff + только с нулевым кол-вом рейсов
    df_onoff_no_trips_it1 = df_onoff[(~(df_onoff.move_id.isin(df_spreads[df_spreads.move_id.notnull()].move_id.unique()))) |
                                 (df_onoff.move_id.isin(df_spreads[df_spreads.sum_num_ot == 0].move_id.unique()))]
    
    df_spreads_it1 = df_spreads.drop('sum_num_ot', axis=1)
    
    
    #======= 2-ая итерация распределения поездок =======
    #пробуем их распределить внутри варианта маршрута (variant_name)
    df_links_variant = pd.merge(df_trip_stops, df_routes[['route_id','trip_id','variant_name']], 
                                how = 'left', on = ['route_id','trip_id'])
    df_links_variant = pd.merge(df_links_variant, df_links_variant, how='inner', on=['route_id'])
    df_links_variant = df_links_variant[(df_links_variant.trip_id_x != df_links_variant.trip_id_y) &
                                        (df_links_variant.variant_name_x == df_links_variant.variant_name_y)]
    df_links_variant = df_links_variant.rename(columns={'trip_id_x':'trip_id_on',
                                                        'trip_id_y':'trip_id_off',
                                                        'stop_sequence_x':'stop_sequence_on', 
                                                        'stop_id_x':'stop_id_on',
                                                        'site_id_x':'site_id_on',
                                                        'stop_sequence_y':'stop_sequence_off', 
                                                        'stop_id_y':'stop_id_off',
                                                        'site_id_y':'site_id_off',
                                                        'variant_name_x':'variant_name'})
                                                        
    #длина конкретной связи для каждого конкретного мвна:
    df_links_variant['trip_on_length'] = df_links_variant.groupby(by = ['trip_id_on']).cum_distance_x.transform(max)
    df_links_variant['link_length'] = abs(df_links_variant.trip_on_length + df_links_variant.cum_distance_y - df_links_variant.cum_distance_x)
    df_links_variant = df_links_variant.drop(['cum_distance_x','cum_distance_y',
                                              'trip_on_length','variant_name_y'], axis=1)

    df_links_variant = df_links_variant[df_links_variant.stop_sequence_off > 1]
    df_links_variant = df_links_variant.sort_values(by = ['route_id','trip_id_on','stop_sequence_on','trip_id_off','stop_sequence_off'])
    
    #распределяем поездки по 2ой итерации
    if alg_type == 'прогнозный':
        df_spreads_variant = pd.merge(df_links_variant, df_onoff_no_trips_it1[['site_id_on','site_id_off','cnt','move_id']],
                                      how = 'inner', on = ['site_id_on','site_id_off'])
        # исключаем дублирование распределения поездок внутри варианта route_id
        df_spreads_variant = pd.merge(df_spreads_variant,
                                      df_spreads_variant[['trip_id_on',
                                                          'stop_sequence_on']].drop_duplicates(subset=['trip_id_on'],
                                                                keep='last').rename(columns={'stop_sequence_on':'stop_seq_on_max'}),
                                      how='left', on = 'trip_id_on')
        df_spreads_variant['stop_seq_diff'] = abs(
                df_spreads_variant['stop_sequence_off'] + df_spreads_variant['stop_seq_on_max'] - df_spreads_variant['stop_sequence_on'])
        df_spreads_variant = df_spreads_variant.sort_values(by=['route_id', 'variant_name', 'move_id', 'stop_seq_diff'])
        df_spreads_variant = df_spreads_variant.drop_duplicates(subset=['route_id', 'variant_name', 'move_id'])
        df_spreads_variant = df_spreads_variant.drop(['stop_seq_on_max','stop_seq_diff'], axis=1)
    else:
        try:
            #если в матрице onoff есть данные о trip_id и trip_id_on != trip_id_off
            df_spreads_on = pd.merge(df_links.drop_duplicates(subset=['route_id','trip_id','stop_sequence_on'], keep='last'),
                                    df_onoff_no_trips_it1[(df_onoff_no_trips_it1.trip_id_on !=
                                              df_onoff_no_trips_it1.trip_id_off)][['route_id','trip_id_on','stop_sequence_on',
                                                                      'stop_id_on','site_id_on','cnt','move_id']],
                                    how = 'left',
                                    left_on = ['route_id','trip_id','stop_sequence_on','stop_id_on','site_id_on'],
                                    right_on = ['route_id','trip_id_on','stop_sequence_on','stop_id_on','site_id_on'])
            df_spreads_on = df_spreads_on.drop('trip_id_on', axis=1)
        
            df_spreads_off = pd.merge(df_links.drop_duplicates(subset=['route_id','trip_id','stop_sequence_off']),
                                     df_onoff_no_trips_it1[(df_onoff_no_trips_it1.trip_id_on !=
                                               df_onoff_no_trips_it1.trip_id_off)][['route_id','trip_id_off','stop_sequence_off',
                                                                       'stop_id_off','site_id_off','cnt','move_id']],
                                     how = 'left',
                                     left_on = ['route_id','trip_id','stop_sequence_off','stop_id_off','site_id_off'],
                                     right_on = ['route_id','trip_id_off','stop_sequence_off','stop_id_off','site_id_off'])
            df_spreads_off = df_spreads_off.drop('trip_id_off', axis=1)
            df_spreads_onoff = df_spreads_on.append(df_spreads_off).drop_duplicates()
            #добавляем кол-во рейсов
            df_spreads_onoff = pd.merge(df_spreads_onoff, df_num_of_trips, how = 'left', on = ['route_id','trip_id'])
            df_spreads_onoff['corr_cnt'] = df_spreads_onoff.cnt
            df_spreads_onoff = df_spreads_onoff[df_spreads_onoff.move_id.notnull()]
            
            df_onoff_no_trips_it1 = df_onoff_no_trips_it1[~(df_onoff_no_trips_it1.move_id.isin(df_spreads_onoff[df_spreads_onoff.move_id.notnull()].move_id.unique()))]
        except:
            pass
            
        df_spreads_variant = pd.merge(df_links_variant, df_onoff_no_trips_it1[['route_id','stop_id_on','site_id_on','stop_id_off',
                                                                          'site_id_off','cnt','move_id']], 
                                     how = 'inner', on = ['route_id','stop_id_on','site_id_on',
                                                          'stop_id_off','site_id_off'])
                                                             
        df_spreads_variant = df_spreads_variant.sort_values(by = ['route_id','move_id','link_length'])
        df_spreads_variant = df_spreads_variant.drop_duplicates(subset = ['route_id','variant_name','move_id'])

    #подтягиваем вход/выход на начальных/конечных
    df_spreads_variant_on = pd.merge(df_spreads_variant[['route_id','trip_id_on','stop_sequence_on',
                                                       'stop_id_on','site_id_on','cnt',
                                                       'move_id','variant_name']].rename(columns={'trip_id_on':'trip_id'}),
                                     df_links.drop_duplicates(subset=['route_id','trip_id','stop_sequence_on'], keep='last'),
                                     how = 'left',
                                     on = ['route_id','trip_id','stop_sequence_on','stop_id_on','site_id_on'])

    df_spreads_variant_off = pd.merge(df_spreads_variant[['route_id','trip_id_off','stop_sequence_off',
                                                       'stop_id_off','site_id_off','cnt',
                                                       'move_id','variant_name']].rename(columns={'trip_id_off':'trip_id'}),
                                      df_links.drop_duplicates(subset=['route_id','trip_id','stop_sequence_off']),
                                      how = 'left',
                                      on = ['route_id','trip_id','stop_sequence_off','stop_id_off','site_id_off'])
                                     
    #объединяем вход и выход
    df_spreads_additional = df_spreads_variant_on.append(df_spreads_variant_off)
    #добавляем кол-во рейсов
    df_spreads_additional = df_spreads_additional[df_spreads_additional.stop_sequence_off.notnull()]
    df_spreads_additional = pd.merge(df_spreads_additional, df_num_of_trips, how = 'left', on = ['route_id','trip_id'])
    #кол-во рейсов по вариантам
    df_variant_num_ot = df_spreads_additional[['route_id','trip_id','variant_name','num_of_trips','move_id','cnt']]
    df_variant_num_ot['variant_num_ot'] = df_variant_num_ot.groupby(by = ['route_id','variant_name','move_id'])['num_of_trips'].transform(max)
    df_part_num_ot = df_variant_num_ot[['route_id','variant_name','move_id','variant_num_ot','cnt']].drop_duplicates()
    #доля суммарного кол-ва рейсов реализующих связь для заданного trip_id
    if alg_type == 'прогнозный':
        df_part_num_ot['part_num_ot'] = df_part_num_ot.variant_num_ot/df_part_num_ot.groupby(by = ['move_id'])['variant_num_ot'].transform(sum)
    else:
        #для связей реализуемых несколькими variant_name в рамках одного route_id
        df_part_num_ot['part_num_ot'] = df_part_num_ot.variant_num_ot/df_part_num_ot.groupby(by = ['route_id',
                                                                    'move_id'])['variant_num_ot'].transform(sum)
    
    #поправка на кол-во пассажиров:
    df_part_num_ot['corr_cnt'] = df_part_num_ot.cnt * df_part_num_ot.part_num_ot
    df_part_num_ot = df_part_num_ot[['route_id','variant_name','move_id','corr_cnt']]
    
    df_spreads_additional = pd.merge(df_spreads_additional, df_part_num_ot, how = 'left',
                                     on = ['route_id','variant_name','move_id'])
    df_spreads_additional = df_spreads_additional.drop('variant_name', axis=1)
    
    try:
        #добавляем распределенные ранее поездки df_spreads_onoff (если в матрице onoff есть trip_id)
        df_spreads_additional = df_spreads_additional.append(df_spreads_onoff)
    except:
        pass
    
    #добавляем новые распределившиеся поездки
    df_spreads_it2 = df_spreads_it1.append(df_spreads_additional)
    df_spreads_it2['corr_cnt'] = df_spreads_it2.corr_cnt.fillna(0)
    df_spreads_it2 = df_spreads_it2.drop(['cnt','link_length'], axis=1)
    df_spreads_it2 = df_spreads_it2.sort_values(by = ['route_id','trip_id','stop_sequence_on','stop_sequence_off','corr_cnt'])
    df_spreads_it2 = df_spreads_it2.drop_duplicates(keep='last')
                                               
                                               
    #======= Нераспределенные по маршрутам поездки из матрицы onoff после 2-ой итерации =======
    df_onoff_no_trips_it2 = df_onoff[~(df_onoff.move_id.isin(df_spreads_it2[df_spreads_it2.move_id.notnull()].move_id.unique()))]
    #добавляем код причины: 0 - для поездки не нашлось ни одного подходящего маршрута из БД UARMS
    df_onoff_no_trips_it2['reason'] = 'no_routes'
    #также сюда добавим распределившиеся поездки, но для которых по всем маршрутам отсутствуют рейсы (код 1)
    df_spreads_it2['sum_num_ot'] = df_spreads_it2.groupby(by = ['move_id'])['num_of_trips'].transform(sum)
    df_onoff_no_num_ot = df_onoff[df_onoff.move_id.isin(df_spreads_it2[df_spreads_it2.sum_num_ot == 0].move_id.unique())]
    df_onoff_no_num_ot['reason'] = 'no_schedules'
    df_onoff_no_trips_it2 = df_onoff_no_trips_it2.append(df_onoff_no_num_ot)
    df_onoff_no_trips_it2 = df_onoff_no_trips_it2.drop('move_id', axis=1)
    df_spreads_it2 = df_spreads_it2.drop('sum_num_ot', axis=1)

    #Кол-во распределенных поездок по 1ой итерации
    move_num_it1 = df_onoff.cnt.sum() - df_onoff_no_trips_it1.cnt.sum()

    #Кол-во распределенных поездок по 2ой итерации
    move_num_it2 = df_onoff_no_trips_it1.cnt.sum() - df_onoff_no_trips_it2.cnt.sum()
    
    return df_spreads_it2, df_onoff_no_trips_it2, move_num_it1, move_num_it2
        
    
#====== Расчет ======
def get_occupancy(df_occupancy, df_capacity):
    '''расчет заполненности салона и загруженности перегонов'''
    df_occupancy['corr_cnt'] = df_occupancy.corr_cnt.fillna(0)
    #Расчет "вход/выход/заполненность салона" по поостановочным перегонам:
    #вход:
    df_occupancy['trip_in'] = df_occupancy.groupby(by = ['route_id','trip_id',
                                                         'stop_sequence_on'])['corr_cnt'].transform(sum)
    #выход:
    df_occupancy['trip_out'] = df_occupancy.groupby(by = ['route_id','trip_id',
                                                          'stop_sequence_off'])['corr_cnt'].transform(sum)
    #формируем поостановочные перегоны
    df_occupancy = df_occupancy.drop_duplicates(subset = ['route_id','trip_id','stop_sequence_on'])
    
    #заполненность салона:
    df_occupancy['trip_volume'] = df_occupancy.groupby(by = ['route_id',
                                  'trip_id'])['trip_in'].cumsum() - df_occupancy.groupby(by = ['route_id',
                                              'trip_id'])['trip_out'].cumsum() + df_occupancy.trip_out
    df_occupancy.trip_volume = round(df_occupancy.trip_volume,2)
    
    #добавляем вместимость
    df_occupancy = pd.merge(df_occupancy, df_capacity, how = 'left', on = ['route_id','trip_id'])
    
    #провозная способность маршрута (trip_id)
    df_occupancy['trip_capacity'] = round(df_occupancy.capacity * df_occupancy.num_of_trips,2)
    
    #загруженность (доля от провозной способности) маршрута на перегоне
    df_occupancy['trip_voc'] = round(df_occupancy.trip_volume / df_occupancy.trip_capacity,2)
    
    #заполненность перегонов
    df_occupancy['stage_volume'] = df_occupancy.groupby(by = ['site_id_on','site_id_off'])['trip_volume'].transform(sum)
    df_occupancy.stage_volume = round(df_occupancy.stage_volume,2)
    
    #провозная способность перегонов
    df_occupancy['stage_capacity'] = df_occupancy.groupby(by = ['site_id_on','site_id_off'])['trip_capacity'].transform(sum)
    
    #загруженность (доля от провозной способности) перегонов
    df_occupancy['stage_voc'] = round(df_occupancy.stage_volume / df_occupancy.stage_capacity,2)
                                              
    df_occupancy = df_occupancy[['route_id','trip_id','stop_sequence_on','stop_id_on','site_id_on','stop_sequence_off','stop_id_off',
                                 'site_id_off','num_of_trips','trip_capacity','trip_in','trip_out','trip_volume','trip_voc',
                                 'stage_capacity','stage_volume','stage_voc']]
                                 
    return df_occupancy


#====== Формирование датасета с информацией о параметрах произведенного расчета ======
def get_info(onoff_dates_interval, db_dates_interval, hours, scenario_id, alg_type, schedule_type, is_hst, drop_trips,  \
             file_name, move_sum, move_num_it1, move_num_it2):
    '''формирование датасета с информацией о параметрах произведенного расчета'''
    # формирование датасета с информацией о параметрах произведенного расчета:
    df_info = pd.DataFrame()
    df_matrix_dates = pd.DataFrame()
    df_db_dates = pd.DataFrame()
    df_mov_info = pd.DataFrame()

    df_info['id сценария'] = [scenario_id]
    df_info['тип алгоритма'] = [alg_type]
    df_info['тип расписания'] = [schedule_type]
    df_info['учет СВТ'] = [is_hst]
    df_info['исключены trip_id'] = drop_trips
    df_info['_index'] = df_info.index

    if onoff_dates_interval != 0:
        df_matrix_dates['даты матрицы ONOFF'] = onoff_dates_interval
        df_matrix_dates['даты матрицы ONOFF'] = df_matrix_dates['даты матрицы ONOFF'].dt.strftime('%Y-%m-%d')
    else:
        df_matrix_dates['прогнозная матрица ONOFF'] = [file_name]
    df_matrix_dates['_index'] = df_matrix_dates.index

    df_db_dates['даты БД UARMS'] = db_dates_interval
    df_db_dates['даты БД UARMS'] = df_db_dates['даты БД UARMS'].dt.strftime('%Y-%m-%d')
    df_db_dates['_index'] = df_db_dates.index

    df_mov_info['временной интервал'] = [str(hours[0]) + ':00 - ' + str(hours[1]) + ':00']
    df_mov_info['число поездок по усред. матрице'] = [round(move_sum, 2)]
    df_mov_info['распределено по 1-ой итерации'] = [round(move_num_it1, 2)]
    df_mov_info['распределено по 2-ой итерации'] = [round(move_num_it2, 2)]
    df_mov_info['остались не распределенными'] = [round(move_sum - move_num_it1 - move_num_it2, 2)]
    df_mov_info['_index'] = df_mov_info.index

    df_info = pd.merge(df_info, df_matrix_dates, how='outer', on='_index')
    df_info = pd.merge(df_info, df_db_dates, how='outer', on='_index')
    df_info = pd.merge(df_info, df_mov_info, how='outer', on='_index')

    df_info = df_info.drop(['_index'], axis=1)

    df_info = df_info.T

    return df_info
    
    
#====== Сохранение результатов ======
def save_results(df_occupancy, df_onoff, df_onoff_no_trips, routes_no_num_ot, df_info, move_num_it1, move_num_it2):
    '''формирование таблицы с информацией и сохранение результатов'''
    #вывод кратких результатов на экран
    print('\t---------------------------------------------')
    print('\tОбщее число входов*: ' + str(round(df_occupancy.trip_in.sum(),2)))
    print('\tОбщее число выходов*: ' + str(round(df_occupancy.trip_out.sum(),2)))
    print('\t---------------------------------------------')
    print('\tОбщее число поездок по матрице onoff: ' + str(round(df_onoff.cnt.sum(),2)))
    print('\tРаспределено поездок по trip_id в 1-ой итерации: ' + str(round(move_num_it1, 2)))
    print('\tРаспределено поездок по trip_id во 2-ой итерации: ' + str(round(move_num_it2, 2)))
    print('\tОстались не распределенными по trip_id: ' + str(round(df_onoff_no_trips.cnt.sum(),2)))
    print('\t---------------------------------------------')
    print('\t*С учетом распределения по 2-ой итерации общее число входов/выходов может быть больше чем общее число поездок по матрице onoff!')

    #Сохранение результатов:
    filename = 'st_occ_results_{}.xlsx'.format(str(datetime.utcnow().date()).replace('-',''))

    writer = pd.ExcelWriter(ensure_dir(str(config.projectPath) + '/data/' + str(filename)), engine='xlsxwriter')
    df_occupancy.to_excel(writer, 'trips_occupancy', encoding='cp1251', index=False)
    df_onoff_no_trips.to_excel(writer, 'onoff_no_trips', encoding='cp1251', index=False)
    routes_no_num_ot.to_excel(writer, 'routes_no_schedule', encoding='cp1251', index=False)
    df_info.to_excel(writer, 'info', encoding='cp1251', header=False)
    writer.save()
