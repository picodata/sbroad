import tarantool from "k6/x/tarantool";
import {randomItem} from 'https://jslib.k6.io/k6-utils/1.1.0/index.js';
import { updateSuccessRate } from '../metrics.js';

let host = "localhost";
if (__ENV.HOST) {
    host = __ENV.HOST;
}

const client = tarantool.connect([host + ":3301"], {"user": "admin", pass: "app-cluster-cookie"})

let ids = Array.from(
    {
        length: 1000000
    },
    (_, id) => id
)

let pattern = `SELECT *
    FROM (
            SELECT "id", "gov_number"
            FROM "vehicle_actual"
            WHERE "sys_op" = 0
            UNION ALL
            SELECT "id", "gov_number"
            FROM "vehicle_history"
            WHERE "sys_op" < 1
        ) as "t1"
    WHERE "id" = ?`

export default () => {
    var resp = tarantool.call(client, "sbroad.execute", [pattern, [randomItem(ids)]]);
    updateSuccessRate(resp);
}
