from datetime import datetime

from modules.main import execute
from modules.db_data import get_scenarios

import tkinter as tk
from tkinter import ttk


def btn_execute_push(onoff_dates,db_dates,hour_start,hour_end,scenario_id,alg_type,schedule_type,is_hst,drop_trips):
    '''parameters to run'''
    #Помещаем вводимые параметры в словарь и передаем в ф-ию run.execute()
    params_to_run = {} 
    params_to_run['onoff_dates'] = onoff_dates
    params_to_run['db_dates'] = db_dates
    params_to_run['hours'] = str(hour_start) + '-' + str(hour_end)
    params_to_run['scenario_id'] = scenario_id
    params_to_run['alg_type'] = alg_type
    params_to_run['schedule_type'] = schedule_type
    params_to_run['is_hst'] = is_hst
    params_to_run['drop_trips'] = drop_trips
    
    execute(params_to_run)
    
    
#Создаем окно tkinter
root = tk.Tk()
root.title("Stages_occupancy")

label_1 = tk.Label(root, text="Даты матрицы ONOFF:")
label_2 = tk.Label(root, text="Даты БД UARMS:")
label_3 = tk.Label(root, text="Временной интервал:")
label_4 = tk.Label(root, text=" - ")
label_5 = tk.Label(root, text="id сценария:")
label_6 = tk.Label(root, text="Тип алгоритма:")
label_7 = tk.Label(root, text="Тип расписания:")
label_8 = tk.Label(root, text="Учитывать СВТ:")
label_9 = tk.Label(root, text="Исключить trip_id:")
btn_execute = tk.Button(root, text= "Выполнить", fg="blue")

onoff_dates = tk.Entry(root, width=23)
onoff_dates.insert(1, str(datetime.utcnow().date()).replace('-',''))
db_dates = tk.Entry(root, width=23)
db_dates.insert(1, str(datetime.utcnow().date()).replace('-',''))
hour_start = ttk.Combobox(root, values=list(range(0,24)), width=6, state='readonly')
hour_start.current(8)
hour_end = ttk.Combobox(root, values=list(range(0,24)), width=6, state='readonly')
hour_end.current(9)
scenario_id = ttk.Combobox(root, values=list(get_scenarios().scenario_id.values), width=20, state='readonly')
scenario_id.current(0)
alg_type = ttk.Combobox(root, values=['прогнозный','фактический'], width=20, state='readonly')
alg_type.current(0)
schedule_type = ttk.Combobox(root, values=['плановое','фактическое'], width=20, state='readonly')
schedule_type.current(0)
is_hst = ttk.Combobox(root, values=['нет','да'], width=20, state='readonly')
is_hst.current(0)
drop_trips = tk.Entry(root, width=23)

label_1.grid(row=0, column=0)
label_2.grid(row=1, column=0)
label_3.grid(row=2, column=0)
label_4.grid(row=2, column=2)
label_5.grid(row=3, column=0)
label_6.grid(row=4, column=0)
label_7.grid(row=5, column=0)
label_8.grid(row=6, column=0)
label_9.grid(row=7, column=0)
btn_execute.grid(row=8, column=1)

onoff_dates.grid(row=0, column=1, columnspan=3)
db_dates.grid(row=1, column=1, columnspan=3)
hour_start.grid(row=2, column=1)
hour_end.grid(row=2, column=3)
scenario_id.grid(row=3, column=1, columnspan=3)
alg_type.grid(row=4, column=1, columnspan=3)
schedule_type.grid(row=5, column=1, columnspan=3)
is_hst.grid(row=6, column=1, columnspan=3)
drop_trips.grid(row=7, column=1, columnspan=3)

btn_execute.bind("<Button-1>", lambda event: btn_execute_push(onoff_dates.get(), 
                                                              db_dates.get(), 
                                                              hour_start.get(),
                                                              hour_end.get(),
                                                              scenario_id.get(),
                                                              alg_type.get(),
                                                              schedule_type.get(),
                                                              is_hst.get(),
                                                              drop_trips.get()))

root.mainloop()