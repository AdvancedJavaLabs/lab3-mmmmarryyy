#!/bin/bash

set -e

echo "Запуск MapReduce для анализа продаж"

REDUCERS=4
BLOCK_SIZE_KB=1024

read -p "Количество reducer-ов [$REDUCERS]: " user_reducers
read -p "Размер блока в KB [$BLOCK_SIZE_KB]: " user_block_size

REDUCERS=${user_reducers:-$REDUCERS}
BLOCK_SIZE_KB=${user_block_size:-$BLOCK_SIZE_KB}

echo "1. Запуск Hadoop кластера..."
docker-compose up -d

echo "Ожидание запуска NameNode..."
while ! curl -s http://localhost:9870 > /dev/null; do
    sleep 2
    echo -n "."
done
echo " Готово!"

echo "Ожидание запуска DataNode..."
while ! curl -s http://localhost:9864 > /dev/null; do
    sleep 2
    echo -n "."
done
echo " Готово!"

echo "Ожидание запуска ResourceManager..."
while ! curl -s http://localhost:8088 > /dev/null; do
    sleep 2
    echo -n "."
done
echo " Готово!"

echo "Ожидание запуска NodeManager..."
while ! curl -s http://localhost:8042 > /dev/null; do
    sleep 2
    echo -n "."
done
echo " Готово!"

echo "2. Сборка проекта..."
./gradlew clean jar

echo "3. Подготовка HDFS..."

docker exec namenode hdfs dfs -rm -r /input /output /output-temp 2>/dev/null || true
docker exec namenode hdfs dfs -mkdir -p /input

echo "4. Копирование CSV файлов в HDFS..."

docker exec namenode rm -rf /tmp/*.csv 2>/dev/null || true

for csv_file in csv_files/*.csv; do
    if [ -f "$csv_file" ]; then
        filename=$(basename "$csv_file")
        docker cp "$csv_file" namenode:/tmp/
        docker exec namenode hdfs dfs -put -f /tmp/"$filename" /input/
    fi
done

echo "5. Подготовка JAR файла..."
JAR_FILE=$(find build/libs -name "*.jar" | head -n 1)

docker cp "$JAR_FILE" namenode:/tmp/sales-app.jar
docker cp "$JAR_FILE" resourcemanager:/tmp/sales-app.jar

echo "6. Запуск MapReduce задания через YARN..."
echo "Параметры: input=/input, output=/output, reducers=$REDUCERS, blockSize=$BLOCK_SIZE_KB KB"

echo "Запуск задания через YARN..."
docker exec resourcemanager yarn jar /tmp/sales-app.jar /input /output $REDUCERS $BLOCK_SIZE_KB

echo "7. Получение результатов..."

if docker exec namenode hdfs dfs -test -e /output/_SUCCESS 2>/dev/null; then
    echo "Задание выполнено"

    mkdir -p results

    > results.txt
    echo "Category,Revenue,Quantity" >> results.txt

    docker exec namenode hdfs dfs -ls /output/part-r-* 2>/dev/null | \
        awk '{print $NF}' | \
        while read file; do
            docker exec namenode hdfs dfs -cat "$file" 2>/dev/null >> results.txt
            echo "" >> results.txt
        done

    echo "Результаты сохранены в results.txt"
else
    echo "Ошибка: задание не завершилось успешно"

    exit 1
fi

echo "Готово!"