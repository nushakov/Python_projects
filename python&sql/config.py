import os
from sqlalchemy import create_engine

#Путь к файлу ноутбука:
projectPath = os.path.abspath('')
projectPath = os.path.abspath(projectPath).replace('\\','/').replace(str('modules'),'')

#Другие часто используемые переменные:
weekdays = {0:'monday', 1:'tuesday', 2:'wednesday', 3:'thursday', 4:'friday', 5:'saturday', 6:'sunday'}

# Подключение к базе данных UARMS:
user = 'tool'
password = 'tool'
name_base='DB'
app_name = 'example'
engine = create_engine('postgresql://{}:{}@{}:5432/{}?application_name={}'.format(user, password, 
                                                                                  'example.my_portfolio.ru', 
                                                                                   name_base,
                                                                                   app_name))