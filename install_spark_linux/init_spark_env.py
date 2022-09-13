# --- Шаг 1. ---
# Загрузка файла spark-3.3.0-bin-hadoop3.tgz

# --- Шаг 2. ---
# Проверка скаченного пакета
# certutil -hashfile .\spark-3.3.0-bin-hadoop3.tgz SHA512

# --- Шаг 3. ---
# Разархивировация пакета и перемещение файлов
# 1). tar -xzf spark-3.3.0-bin-hadoop3.tgz
# 2). mv spark-3.3.0-bin-hadoop3 spark
# 3). Перемещаем файлы по указанному пути /home/pavel/

# --- Шаг 4. ---
# Настройка переменных окружения
# 1). nano ~/.bashrc
# 2). export SPARK_HOME=/home/pavel/spark
# 3). export PATH=$PATH:$SPARK_HOME/bin
# 4). source ~/.bashrc

# --- Шаг 5. ---
# Тест в терминале
# pyspark
# quit()

# --- Шаг 6. ---
# Подготовка рабочего проекта Python
# 1). Создать папку
# 2). python3 -m venv .venv
# 3). source .venv/bin/activate
# 4). pip install jupyterlab
# 5). jupyter-lab

# --- Шаг 7. ---
# Создание файла init_spark_env.py
# import sys, os
# sys.path.append("/home/pavel/spark/python/lib/py4j-0.10.9.5-src.zip")
# sys.path.append("/home/pavel/spark/python")
# os.environ["SPARK_HOME"] = "/home/pavel/spark"

# --- Шаг 8. ---
# Импорт файла init_spark_env.py в рабочий ноутбук


import sys, os

sys.path.append("/home/pavel/spark/python/lib/py4j-0.10.9.5-src.zip")
sys.path.append("/home/pavel/spark/python")

os.environ["SPARK_HOME"] = "/home/pavel/spark"