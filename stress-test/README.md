# Нагрузочное тестирование

Для проведения НТ необходимо:

1. Запустить [тестовое приложение](../test_app)
    ```bash
    cd ../test_app && cartridge start
    ``` 
   
   Если кластер не сконфигурирован, то настроить через панель администрирования или командой:
   ```bash
      cartridge replicasets setup --file replicasets.yml --bootstrap-vshard
   ```
2. Сгенерировать тестовые данные с помощью скрипта:
    ```bash
    ./data_generator.lua 1000
    ```
    в параметре передается кол-во записей для генерации
3. Запустить скрипт [k6](https://k6.io/docs/getting-started/running-k6/) c [dtm модулем](https://gitlab.com/picodata/arenadata/asbest/-/tree/master/xk6-plugin-dtm) ([как собрать](https://k6.io/blog/extending-k6-with-xk6/))
    ```bash
   k6 -u 10 -d 1m k6.js
   ```