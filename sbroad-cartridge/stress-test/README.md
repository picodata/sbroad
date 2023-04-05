# Load testing
## Using docker-compose
Run from sbroad directory
```
make stress test=projection
```
Parameter `test` is type of stress test (corresponding to the name of folder). As result you will see file `k6_summary.lua` with summarized metrics inside chosen stress test folder.
## Using k6 and tarantool
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
1. Build [k6](https://k6.io/docs/getting-started/running-k6/)
   ```bash
   xk6 build --with github.com/WeCodingNow/xk6-tarantool
   ```

1. Run the [k6](https://k6.io/docs/getting-started/running-k6/) script
    ```bash
   ./k6 run -u 10 -d 1m k6.js --summary-export k6_summary.json
   ```
   **Note:**
If you run stress tests sequentially, you may need to do `cartridge clean` before next test run because tests might have
conflicting schemas.

## Compare results
If you need to compare results serveral test, you may run
```
lua compare.lua path_to_k6_summary_1.json path_to_k6_summary_2.json
```
It considered that first result must have lower rps to get 0 exit code. Example of output:
```
$ lua compare.lua path_to_k6_summary_2.json path_to_k6_summary_1.json
+----------------+-----------------+------------------+
| metrics        | left result     | right result     |
+----------------+-----------------+------------------+
| Success value  | 0.3333689238811 | 0.33428112947271 |
| Passes         | 71812           | 71244            |
| Fails          | 143601          | 141882           |
| VUs            | 10              | 10               |
| Iterations rps | 3539.5409122402 | 3503.5619844057  |
+----------------+-----------------+------------------+
Left (first) result must have lower or equal rps
```
