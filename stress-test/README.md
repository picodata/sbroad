# Load testing

1. Run [test application](../test_app)
    ```bash
    cd ../test_app && cartridge start
    ``` 
   
   If the cluster is not configured then use the the web-based administrative panel or run the following command to configure it:
   ```bash
      cartridge replicasets setup --file replicasets.yml --bootstrap-vshard
   ```
2. Generate test data and define the number of records:
    ```bash
    ./data_generator.lua 1000
    ```
3. Build [k6](https://k6.io/docs/getting-started/running-k6/) with the [dtm module](https://gitlab.com/picodata/arenadata/asbest/-/tree/master/xk6-plugin-dtm)
   ```
   xk6 build --with xk6-plugin-dtm=gitlab.com/picodata/arenadata/asbest/-/tree/master/xk6-plugin-dtm --with github.com/hackfeed/xk6-tarantool
   ```

4. Run the [k6](https://k6.io/docs/getting-started/running-k6/) script 
    ```bash
   k6 -u 10 -d 1m k6.js
   ```
