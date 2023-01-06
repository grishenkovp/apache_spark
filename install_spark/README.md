## Установка Apache Spark

### Установка Java
* sudo apt-get update
* sudo apt-get install default-jre
* java -version

### 1. Загрузка файла 
* wget -q https://archive.apache.org/dist/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz

### 2. Проверка скаченного пакета (Windows)
* certutil -hashfile .\spark-3.3.1-bin-hadoop3.tgz SHA512

### 3. Разархивация пакета и перемещение файлов
* tar -xzf spark-3.3.1-bin-hadoop3.tgz
* mv spark-3.3.0-bin-hadoop3 /home/pavel/spark

### 4. Настройка переменных окружения
* nano ~/.bashrc
* export SPARK_HOME=/home/pavel/spark
* export PATH=$PATH:$SPARK_HOME/bin
* source ~/.bashrc

### 5. Тест в терминале
* pyspark
* quit()

### 6. Подготовка рабочего проекта Python
* mkdir test_spark
* cd test_spark
* python3 -m venv .venv
* source .venv/bin/activate
* pip install jupyterlab
* jupyter-lab

### 7. Создание файла init_spark_env.py

### 8. Импорт файла init_spark_env.py в рабочий ноутбук

