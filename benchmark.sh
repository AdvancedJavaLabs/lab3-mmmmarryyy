#!/bin/bash

set -e

echo "Запуск бенчмарков"
echo "Дата: $(date)"

REDUCERS_LIST=(16 8 4 2 1)
BLOCK_SIZES_KB=(64 128 256 512 1024 2048 4096)

RESULTS_FILE="benchmark_results.csv"
echo "reducers,block_size_kb,time_sec" > "$RESULTS_FILE"

wait_for_cluster() {
    echo "Ожидание запуска всех компонентов Hadoop..."

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
}

prepare_hdfs_input_once() {
    echo "Подготовка входных данных в HDFS"

    docker exec namenode hdfs dfs -rm -r /input /output /output-temp 2>/dev/null || true
    docker exec namenode hdfs dfs -mkdir -p /input
    docker exec namenode rm -rf /tmp/*.csv 2>/dev/null || true

    for csv_file in csv_files/*.csv; do
        if [ -f "$csv_file" ]; then
            filename=$(basename "$csv_file")
            docker cp "$csv_file" namenode:/tmp/
            docker exec namenode hdfs dfs -put -f /tmp/"$filename" /input/ > /dev/null 2>&1
        fi
    done
    echo "Входные данные готовы"
}

clean_output_between_tests() {
    echo "Очистка выходных данных"

    docker exec namenode hdfs dfs -rm -r /output /output-temp 2>/dev/null || true
}

run_test() {
    local reducers=$1
    local block_size=$2

    echo -e "Тест: reducers=$reducers, block_size=$block_size KB"

    clean_output_between_tests

    local START_TIME=$(date +%s)

    echo "Запуск MapReduce задания..."
    if docker exec namenode hadoop jar /tmp/sales-app.jar /input /output $reducers $block_size > /tmp/hadoop_benchmark.log 2>&1; then

        if docker exec namenode hdfs dfs -test -e /output/_SUCCESS 2>/dev/null; then
            local success=1
            echo "Задание завершилось"
        else
            local success=0
            echo "Задание завершилось, но _SUCCESS не найден"
        fi
    else
        local success=0
        echo "Ошибка выполнения задания"
    fi

    local END_TIME=$(date +%s)
    local duration=$((END_TIME - START_TIME))

    echo "$reducers,$block_size,$duration" >> "$RESULTS_FILE"

    if [ $success -eq 1 ]; then
        echo "Успешно! Время: ${duration}с"
    else
        echo "Ошибка! Время: ${duration}с"
        echo "Последние строки лога:"
        tail -10 /tmp/hadoop_benchmark.log
    fi

    sleep 2
}

main() {
    echo "Запуск Hadoop кластера..."
    docker-compose up -d

    wait_for_cluster

    echo "Сборка проекта..."
    ./gradlew clean jar

    JAR_FILE=$(find build/libs -name "*.jar" | head -n 1)
    if [ -z "$JAR_FILE" ]; then
        echo "Ошибка: JAR файл не найден!"
        exit 1
    fi

    echo "Копирование JAR файла в контейнер..."
    docker cp "$JAR_FILE" namenode:/tmp/sales-app.jar

    prepare_hdfs_input_once

    for reducers in "${REDUCERS_LIST[@]}"; do
        for block_size in "${BLOCK_SIZES_KB[@]}"; do
            run_test $reducers $block_size
        done
    done
}

main