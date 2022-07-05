const xml = require("xml");
const mysql = require('mysql');
const date = require('date-and-time');
const fs = require('fs');

require('dotenv').config();

const conn = mysql.createConnection({
    host: process.env.MYSQL_HOST,
    user: process.env.MYSQL_USER,
    password: process.env.MYSQL_PASSWD,
    database: process.env.MYSQL_DBNAME,
});

const getMaxDate = () => {
    return new Promise((resolve, reject) => {
        return conn.connect(err => {
            if (err) return reject(err);

            const sql = "SELECT MAX(`date`) as max_date FROM `events`";
            return conn.query(sql, (err, result) => {
                if (err) return reject(err);

                const maxDate = result[0]['max_date'];
                if (!!maxDate) return resolve(maxDate);
                else return null;
            });
        });
    });
}

const getEvents = (maxDate) => {
    return new Promise((resolve, reject) => {
        if (!conn) return reject('MySQL not connected.');

        const strDate = date.format(maxDate, 'YYYY-MM-DD HH:mm:ss');
        const daysExport = process.env.DAYS_EXPORT ? process.env.DAYS_EXPORT : 14;
        const sql = "SELECT * FROM `events` WHERE date BETWEEN DATE_SUB('"
            + strDate + "', INTERVAL " + daysExport + " DAY) AND '"
            + strDate + "' ORDER BY `date` DESC";

        return conn.query(sql, (err, result) => {
            if (err) return reject(err);

            return resolve(result);
        });
    });
}

async function main() {
    const maxDate = await getMaxDate();
    let resultXML = [
        {
            response: [
                { _attr: { error: "false", message: "" } },
                { events: [] }
            ]
        }
    ];
    if (!maxDate) {
        console.log('No records.');
    } else {
        const eventsDatabase = await getEvents(maxDate);
        const events = eventsDatabase.map(event => {
            return {
                event: [
                    { name: [event.title] },
                    { currency: [event.currency] },
                    { impact: [event.impact] },
                    { previous: [event.previous] },
                    { date: [date.format(event.date, 'YYYY, MMMM D, HH:mm')] },
                ]
            };
        });
        resultXML = [
            {
                response: [
                    { _attr: { error: "false", message: "" } },
                    { events }
                ]
            }
        ];
    }
    let result = '<?xml version="1.0" encoding="UTF-8"?>\n';
    result = result + xml(resultXML, true);

    fs.writeFileSync('./newevents.xml', result)
    console.log('Exported newevents.xml');
}

main()
    .then(() => process.exit(0))
    .catch((e) => {
        console.log(e);
        process.exit(1);
    });
