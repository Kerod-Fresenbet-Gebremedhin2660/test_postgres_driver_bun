import {CustomDriver, type TDBConfig} from "./driver.ts"

const config: TDBConfig = {
    "host": "localhost",
    "port": 5432,
    "user": "postgres",
    "password": "postgres",
    "database": "test_database"
};
const db = new CustomDriver(config);


//@ts-ignore
db.connect((err, res) => {
    if (err) {
        console.log("CONNECTION ERROR");
        return;
    }
    console.log(res); // connected
});

//@ts-ignore
db.query("select * from users", (err, res) => {
    if (err) {
        console.log("ERROR OCCURRED DURING QUERY.");
        return;
    }
    console.log(res);
    db.close();
});