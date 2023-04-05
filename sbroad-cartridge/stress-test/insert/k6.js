import tarantool from "k6/x/tarantool";
import {uuidv4} from 'https://jslib.k6.io/k6-utils/1.1.0/index.js';
import { callTarantool } from '../metrics.js';

let host = "localhost";
if (__ENV.HOST) {
    host = __ENV.HOST;
}

const clients = [
    tarantool.connect([host + ":3301"], {"user": "admin", pass: "app-cluster-cookie"}),
    tarantool.connect([host + ":3306"], {"user": "admin", pass: "app-cluster-cookie"}),
    tarantool.connect([host + ":3307"], {"user": "admin", pass: "app-cluster-cookie"}),
    tarantool.connect([host + ":3308"], {"user": "admin", pass: "app-cluster-cookie"})
]

export let current_server = 0

function get_client() {
    let c = clients[current_server]
    current_server += 1
    if (current_server >= clients.length) {
        current_server = 0
    }
    return c
}

let pattern = `INSERT INTO "t" ("id", "name", "product_units") VALUES (?, ?, ?)`

export default () => {
    callTarantool(get_client(), "sbroad.execute", [pattern, [uuidv4(), "123", 1]]);
}
