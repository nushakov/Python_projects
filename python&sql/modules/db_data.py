import pandas as pd

import config


#Функции, включающие sql-запросы таблиц из БД:
def get_routes(db_date, scenario_id, is_hst, drop_trips):
    '''данные о маршрутах'''
    _whats_hst = lambda is_hst: ''' OR transport_type in ('М','ММ','МД','МЦ','Эл')''' if is_hst == 1 else ''
    _str_hst = _whats_hst(is_hst)
    
    _whats_drop_trips = lambda drop_trips: ' AND trip_id not in ' + str(drop_trips) + ' ' if len(drop_trips) > 0 else ''
    _str_drop_trips = _whats_drop_trips(drop_trips)
        
    sql_query = """SELECT mvn, trip_id, route_id, transport_type, variant_name
                   FROM routes.mvns_union('{0}',{1})
                   WHERE ((((scenario_id = 1 AND ((registry_type = 'муниципальный') OR 
                          (registry_type = 'межсубъектный' AND agency_id = 0)))
                           OR scenario_id != 1)
                         AND transport_type in ('А','Тб','Тм')){2}){3}
                   ORDER BY route_id, trip_id""".format(db_date, scenario_id, _str_hst, _str_drop_trips)

    return pd.read_sql_query(sql_query, con=config.engine)


def get_stops(db_date, scenario_id):
    '''данные об остановках'''
    sql_query = """SELECT stop_id, log_id,
                          CASE
                              WHEN site_id is not null
                                  THEN site_id
                              ELSE (stop_id*(-1))
                          END site_id,                        
                          st_y(geometry) stop_lat, st_x(geometry) stop_lon
                   FROM routes.stops('{0}',{1})""".format(db_date, scenario_id)
        
    return pd.read_sql_query(sql_query, con=config.engine)


def get_trip_stops(db_date, scenario_id):
    '''данные о последовательностях остановок'''
    sql_query = """SELECT r_m_d.route_id, r_m_d.trip_id, 
                          r_ts_d.stop_sequence, r_ts_d.stop_id, 
                          r_ts_d.stop_length_graph AS distance, r_ts_d.log_id
                   FROM routes.mvns_union('{0}',{1}) r_m_d
                    INNER JOIN routes.trip_stops_union('{0}',{1}) r_ts_d USING(trip_id)
                    LEFT JOIN routes.stops('{0}',{1}) r_st_d USING(stop_id)""".format(db_date, scenario_id)
    df_trip_stops = pd.read_sql_query(sql_query, con=config.engine)
    
    return df_trip_stops
    
    
def get_num_of_trips(db_date, date, scenario_id, hours, is_additional):
    '''данные о запланированном расписании'''
    _whats_word = lambda start_hour, end_hour: 'OR' if start_hour >= end_hour else 'AND'
    _union_word = _whats_word(hours[0], hours[1])
    
    time = lambda hour: '0' if len(str(hour)) == 1 else ''
    
    if not is_additional:
        _str_add_1 = """, r_se_d AS (SELECT service_id, route_id
                                     FROM routes.services('{0}')
                                     WHERE {1} = true
                                      AND (service_date_start <= '{2}' OR service_date_start is null)
                                      AND (service_date_end >= '{2}' OR service_date_end is null))""".format(db_date, 
                                                                                config.weekdays[date.weekday()], date)
        _str_add_2 = """INNER JOIN r_se_d USING (service_id)"""
        _parameter = 'num_of_trips'
    else:
        _str_add_1 = ''
        _str_add_2 = ''
        _parameter = 'log_id'
        
    sql_query = """WITH 
                     r_m_d AS (SELECT route_id, trip_id
                               FROM routes.mvns_union('{0}',{1})),
                     r_i_d AS (SELECT num_of_trips, trip_id, service_id, log_id
                               FROM routes.intervals_union('{0}',{1})
                               WHERE time >= '{2}' {4} time < '{3}'
                                AND num_of_trips != 0){5}
                    SELECT  r_m_d.route_id, r_m_d.trip_id, SUM(r_i_d.num_of_trips) num_of_trips, r_i_d.log_id
                    FROM r_m_d
                     INNER JOIN r_i_d USING (trip_id)
                     {6}
                    GROUP BY r_m_d.route_id, r_m_d.trip_id, r_i_d.num_of_trips, r_i_d.log_id
                    ORDER BY r_m_d.route_id, r_m_d.trip_id, r_i_d.{7} desc""".format(db_date, scenario_id, time(hours[0])+str(hours[0])+':00:00', 
                                                 time(hours[1])+str(hours[1])+':00:00', _union_word, _str_add_1, _str_add_2, _parameter)
                                                 
    return pd.read_sql_query(sql_query, con=config.engine)


def get_capacity(db_date, date, scenario_id):
    '''данные о количестве и вместимости транспортных средств'''
    sql_query = """WITH
                     r_v_d AS (SELECT route_id, service_id, lc lc_count, mc mc_count, hc hc_count,
                                      ehc ehc_count, single single_count, double double_count,
                                      (lc+mc+hc+ehc+single+double) total_vechicles
                               FROM routes.vehicles_union('{0}',{1})
                               WHERE (lc+mc+hc+ehc+single+double) > 0),
                     r_m_d AS (SELECT route_id, trip_id, transport_type, agency_id
                               FROM routes.mvns_union('{0}',{1})),
                     r_se_d AS (SELECT service_id
                                FROM routes.services('{0}')
                                WHERE {2} = true
                                 AND (service_date_start <= '{3}' OR service_date_start is null)
                                 AND (service_date_end >= '{3}' OR service_date_end is null))
                    SELECT r_m_d.route_id, r_m_d.trip_id, r_m_d.transport_type,
                           ((lc*lc_count+mc*mc_count+hc*hc_count+ehc*ehc_count+single*single_count+double*double_count)/
                                                                                             total_vechicles) capacity
                    FROM r_m_d
                     INNER JOIN r_v_d USING(route_id)
                     INNER JOIN r_se_d USING(service_id)
                     INNER JOIN agencies.vehicle_capacities a_vc USING (agency_id, transport_type)
                    GROUP BY  r_m_d.route_id, r_m_d.trip_id, r_m_d.transport_type, capacity
                    ORDER BY r_m_d.route_id, r_m_d.trip_id, capacity desc""".format(db_date, scenario_id, config.weekdays[date.weekday()], date)
    
    return pd.read_sql_query(sql_query, con=config.engine)
    

def get_capacities_by_types():
    '''Усредненные вместимости по видам транспорта'''
    sql_query_ngpt = """SELECT transport_type, border_level as capacity
                        FROM agencies.transport_capacities
                        WHERE capacity_class = 'БВ'"""
                        
    sql_query_hst = """SELECT transport_type, train as capacity
                       FROM agencies.vehicle_capacities
                       WHERE transport_type in ('М','ММ','МД','МЦ','Эл')"""
                       
    return pd.read_sql_query(sql_query_ngpt, con=config.engine), pd.read_sql_query(sql_query_hst, con=config.engine)
    

#Возможные варианты сценариев в базе:
def get_scenarios():
    '''данные о сценариях'''
    sql_query = """SELECT *
                   FROM public.scenarios"""
        
    return pd.read_sql_query(sql_query, con=config.engine)
    
    
def get_hc_capacities(engine):
    '''данные о сценариях'''
    sql_query = """SELECT transport_type, comfortable_level AS capacity_left, border_level AS capacity
                   FROM agencies.transport_capacities
                   WHERE capacity_class = 'БВ'"""
        
    return pd.read_sql_query(sql_query, con=engine)
    

#Исключение трипов из расчета:
def get_mvns_update(db_date, scenario_id, trips_to_drop, engine):
    '''данные о мвнах'''
    sql_query = """SELECT mvn, route_id
                   FROM routes.mvns('{0}',{1})
                   WHERE agency_group IN ('Мосгортранс', 'commercial')
                    AND trip_id NOT IN ({2})""".format(db_date, scenario_id, trips_to_drop)

    return pd.read_sql_query(sql_query, con=engine)
