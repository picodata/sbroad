# Load testing

1. Run [test application](../test_app)
    ```bash
    cd ../test_app && cartridge start
    ``` 
   
   If the cluster is not configured then use the the web-based administrative panel or run the following command to configure it:
   ```bash
      cartridge replicasets setup --file replicasets.yml --bootstrap-vshard
   ```
1. Open a folder with a stress test. Initialize the schema and (optionally) populate the data with a define the number of records:
    ```bash
    ./init.lua 1000
    ```
1. Clone `asbest` repository to an arbitrary local directory
   ```
   git clone git@gitlab.com:picodata/arenadata/asbest.git
   cd asbest
   ```
1. Build [k6](https://k6.io/docs/getting-started/running-k6/) with the [dtm module](https://gitlab.com/picodata/arenadata/asbest/-/tree/master/xk6-plugin-dtm)
   ```
   xk6 build v0.32.0 --with xk6-plugin-dtm="$(pwd)/xk6-plugin-dtm" --with github.com/hackfeed/xk6-tarantool
   ```

1. Run the [k6](https://k6.io/docs/getting-started/running-k6/) script 
    ```bash
   ./k6 run -u 10 -d 1m k6.js
   ```
