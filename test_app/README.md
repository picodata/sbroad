# Cartridge integration test application

The application was created for integration testing of the `sbroad` library. It may also serve as a usage example of `sbroad`.

## Build the application

``` bash
cd .. && make build_test_app
```

## Run test application

```bash
cartridge start
 ``` 

## Architecture

The application includes two roles:

- `api`
- `storage`

The `api` role has two functions: 

- `query(sql)`
- `insert_record(space, values_map)`

The `query` function executes the sql query from the `sql` parameter.

The `insert_record` function detects the bucket from `values_map` parameter and calls `insert_map` function on the bucket storage. 

The `storage` role has the `insert_map(space, values_map)` function, which inserts map of values `values_map` into `space`.

## General observations

`Sbroad` library uses internal lua functions in the cartridge executor and preloads them with `load_lua_extra_function` call in the `init` cartridge function.

As the `sbroad` library caches the cluster cartridge schema internally, any `sbroad` function that is called checks the internal cluster schema, and if that is empty it loads the schema from the main app. If the app schema was updated then the internal cache needs to be cleared. To clear the cache we need to add the `invalidate_caching_schema` call to the `apply_config` cartridge function.

## Local load testing

More information in [stress-test](../stress-test) folder.
