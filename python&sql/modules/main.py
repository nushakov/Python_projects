import pandas as pd

from modules.inputs import check_params
from modules.calculates import get_db_tables, get_dsets_tables, get_additional_num_ot, \
                               get_spreads, get_occupancy, get_info, save_results

#отключить предупреждения pandas (так быстрее считает!!!):
pd.options.mode.chained_assignment = None


def execute(parameters):    
    '''Проверка и запуск расчета'''
    #Проверка и обработка введенных параметров для расчета
    print('\nПроверка введенных параметров')
    onoff_dates_interval, db_dates_interval, hours, scenario_id, \
    alg_type, schedule_type, is_hst, drop_trips = check_params(parameters)


    #Выгрузка усредненных данных из датасетов (матрица onoff, фактическое расписание)
    print('\nВыгрузка данных из датасетов')
    df_fact_schedule, df_onoff, file_name = get_dsets_tables(onoff_dates_interval, db_dates_interval, 
                                                             hours, schedule_type)
    
    
    #Выгрузка усредненных данных БД UARMS
    print('\nВыгрузка данных из БД UARMS')
    df_routes, df_stops, df_trip_stops, df_num_of_trips, df_capacity = get_db_tables(db_dates_interval, hours,scenario_id,
                                                                                     schedule_type, is_hst,drop_trips)
                              
    if schedule_type == 'фактическое':
        df_num_of_trips = df_fact_schedule
        routes_no_num_ot = pd.DataFrame()
    else:
        #Попытка подтянуть num_of_trips для маршрутов у которых они равны 0, но эти маршруты есть в onoffmatrix
        df_num_of_trips, routes_no_num_ot = get_additional_num_ot(df_num_of_trips, df_onoff, db_dates_interval,
                                                                  scenario_id, hours)
                                                                  
                                                                 
    #Распределение поездок из матрицы onoff по маршрутам
    df_spreads, df_onoff_no_trips, move_num_it1, move_num_it2 = get_spreads(df_routes, df_trip_stops, df_stops, \
                                                                      df_num_of_trips, df_onoff, alg_type)
    
    
    #Расчет заполненности салона и загруженности перегонов:
    df_occupancy = get_occupancy(df_spreads, df_capacity)


    #Формирование датасета с информацией о расчете:
    df_info = get_info(onoff_dates_interval, db_dates_interval, hours, scenario_id, alg_type, schedule_type, is_hst, \
                       drop_trips, file_name, df_onoff.cnt.sum(), move_num_it1, move_num_it2)
    
    #Сохранение результатов:
    save_results(df_occupancy, df_onoff, df_onoff_no_trips, routes_no_num_ot, df_info, move_num_it1, move_num_it2)