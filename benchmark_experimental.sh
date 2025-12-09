#!/bin/bash

set -e

echo "Запуск бенчмарков"
echo "Дата: $(date)"

REDUCERS_LIST=(16 8 4 2 1)
BLOCK_SIZES_KB=(4096 8192 16384 32768)

RESULTS_FILE="results/benchmark_results_2048_2_enlarged.csv"
#echo "reducers,block_size_kb,time_sec" > "$RESULTS_FILE"

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

    for csv_file in csv_files_enlarged/*.csv; do
        if [ -f "$csv_file" ]; then
            filename=$(basename "$csv_file")
            docker cp "$csv_file" namenode:/tmp/
            docker exec namenode hdfs dfs -put -f /tmp/"$filename" /input/
        fi
    done
}

clean_output_between_tests() {
    echo "Очистка выходных данных"

    docker exec namenode hdfs dfs -rm -r /output /output-temp 2>/dev/null || true
}

get_yarn_app_id() {
    local app_info=$(docker exec resourcemanager yarn application -list 2>/dev/null | grep "sales" | tail -1)
    if [ ! -z "$app_info" ]; then
        echo "$app_info" | awk '{print $1}'
    else
        echo ""
    fi
}

get_yarn_app_status() {
    local app_id=$1
    if [ ! -z "$app_id" ]; then
        docker exec resourcemanager yarn application -status $app_id 2>/dev/null | grep "State" | head -1 | awk '{print $2}'
    else
        echo "UNKNOWN"
    fi
}

run_test() {
    local reducers=$1
    local block_size=$2

    echo -e "Тест: reducers=$reducers, block_size=$block_size KB"

    clean_output_between_tests

    local START_TIME=$(date +%s)

    echo "Запуск MapReduce задания через YARN..."

    if docker exec resourcemanager yarn jar /tmp/sales-app.jar /input /output $reducers $block_size > /tmp/hadoop_benchmark.log 2>&1; then
        echo "Задание отправлено в YARN"

        local max_wait=300
        local waited=0
        local job_completed=false

        while [ $waited -lt $max_wait ]; do
            sleep 5
            waited=$((waited + 5))

            if docker exec namenode hdfs dfs -test -e /output/_SUCCESS 2>/dev/null; then
                job_completed=true
                echo "Задание успешно завершено (найдено _SUCCESS)"
                break
            fi

            if docker exec namenode hdfs dfs -test -d /output 2>/dev/null && \
               docker exec namenode hdfs dfs -ls /output/part* 2>/dev/null | grep -q part; then
                echo "Выходные файлы найдены (возможно задание завершилось без _SUCCESS)"
                job_completed=true
                break
            fi

            echo -n "."

            local current_apps=$(docker exec resourcemanager yarn application -list 2>/dev/null | grep -c "sales")
            if [ $current_apps -eq 0 ] && [ $waited -gt 30 ]; then
                echo "Задание не найдено в YARN после 30 секунд"
                break
            fi
        done

        local app_id=$(get_yarn_app_id)
        local app_status=$(get_yarn_app_status "$app_id")

        if [ "$job_completed" = true ] || [ "$app_status" = "SUCCEEDED" ]; then
            local success=1
        else
            local success=0
            echo "Задание не завершилось за $max_wait секунд"
        fi
    else
        local success=0
        echo "Ошибка при отправке задания в YARN"
    fi

    local END_TIME=$(date +%s)
    local duration=$((END_TIME - START_TIME))

    if [ $success -eq 1 ]; then
        echo "$reducers,$block_size,$duration" >> "$RESULTS_FILE"
        echo "Успешно! Время: ${duration}с"
    else
        echo "Ошибка! Время: ${duration}с"
        echo "Последние строки лога:"
        tail -20 /tmp/hadoop_benchmark.log
    fi

    echo "Пауза между тестами..."
    sleep 10
}

main() {
    echo "Запуск Hadoop кластера..."
    docker-compose up -d

    wait_for_cluster

    echo "Дополнительная пауза для стабилизации кластера..."
    sleep 10

    echo "Сборка проекта..."
    ./gradlew clean jar

    JAR_FILE=$(find build/libs -name "*.jar" | head -n 1)
    if [ -z "$JAR_FILE" ]; then
        echo "Ошибка: JAR файл не найден!"
        exit 1
    fi

    echo "Копирование JAR файла в контейнеры..."
    docker cp "$JAR_FILE" namenode:/tmp/sales-app.jar
    docker cp "$JAR_FILE" resourcemanager:/tmp/sales-app.jar

    prepare_hdfs_input_once

    echo -e "Начало выполнения бенчмарков..."
    echo "Количество тестов: ${#REDUCERS_LIST[@]} reducers × ${#BLOCK_SIZES_KB[@]} block sizes = $(( ${#REDUCERS_LIST[@]} * ${#BLOCK_SIZES_KB[@]} ))"

    for reducers in "${REDUCERS_LIST[@]}"; do
        for block_size in "${BLOCK_SIZES_KB[@]}"; do
            run_test $reducers $block_size
        done
    done

    echo -e "Бенчмарки завершены. Результаты сохранены в $RESULTS_FILE"
}

trap 'echo "Прерывание выполнения..."; docker-compose down; exit 1' INT TERM

main