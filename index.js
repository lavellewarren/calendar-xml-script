const https = require('https');
const xml2js = require('xml2js');
const parser = new xml2js.Parser({ attrkey: "ATTR" });
const mysql = require('mysql');
const date = require('date-and-time');
require('dotenv').config();

const fetchEventsXMLData = (url) => {
    return new Promise((resolve, reject) => {
        var xmlData = [];
        https.get(url, res => {
            let data = '';
            res.on('data', stream => {
                data += stream;
            }).on('end', () => {
                parser.parseString(data, (error, result) => {
                    if(error === null) {
                        result.response.events[0].event.forEach(event => {
                            xmlData.push({
                                date: event.date[0],
                                name: event.name[0],
                                impact: event.impact[0],
                                previous: event.previous[0],
                                currency: event.currency[0],
                            });
                        });
                        resolve(xmlData);
                    } else {
                        reject(error);
                    }
                });
            }).on('error', reject);
        });
    })
}

const fetchEventsXMLData_2 = (url) => {
    return new Promise((resolve, reject) => {
        var xmlData = [];
        https.get(url, res => {
            let data = '';
            res.on('data', stream => {
                data += stream;
            }).on('end', () => {
                parser.parseString(data, (error, result) => {
                    if(error === null) {
                        result.weeklyevents.event.forEach(event => {
                            xmlData.push({
                                title: event.title[0],
                                country: event.country[0],
                                date: event.date[0],
                                time: event.time[0],
                                impact: event.impact[0],
                                forecast: event.forecast[0],
                                previous: event.previous[0],
                            });
                        });
                        resolve(xmlData);
                    } else {
                        reject(error);
                    }
                });
            }).on('error', reject);
        });
    })
}

const conn = mysql.createConnection({
    host: process.env.MYSQL_HOST,
    user: process.env.MYSQL_USER,
    password: process.env.MYSQL_PASSWD,
    database: process.env.MYSQL_DBNAME,
});

const addEventsData = (values) => {
    return new Promise((resolve, reject) => {
        return conn.connect(err => {
            if (err) return reject(err);

            const sql = "INSERT INTO events (date, title, currency, previous, forecast, impact) VALUES ?";
            return conn.query(sql, [values], (err, result) => {
                if (err) return reject(err);

                return resolve(result.affectedRows);
            });
        });
    });
}

const parseDate_1 = str_date => {
    return date.parse(str_date, 'YYYY, MMMM D, HH:mm');
}

const parseDate_2 = (str_date, str_time) => {
    const len = str_time.length;
    return date.parse(`${str_date} ${str_time.slice(0, len - 2)} ${str_time.slice(len - 2).toUpperCase()}`, 'MM-DD-YYYY h:mm A');
}

const isSameObj = (obj_1, obj_2) => {
    return  obj_1.title === obj_2.title &&
            obj_1.currency === obj_2.currency &&
            obj_1.previous === obj_2.previous &&
            obj_1.impact === obj_2.impact &&
            obj_1.date.getTime() === obj_2.date.getTime();
}

async function main() {
    const eventsData_1 = await fetchEventsXMLData(
        process.env.XML_API_1
    ).then(data => {
        return(data);
    }).catch(err => {
        console.log(err);
        return [];
    });
    const eventsData_2 = await fetchEventsXMLData_2(
        process.env.XML_API_2
    ).then(data => {
        return(data);
    }).catch(err => {
        console.log(err);
        return [];
    });
    const values_1 = eventsData_1.map(event => {
        return {
            title: event.name,
            impact: event.impact,
            previous: event.previous,
            currency: event.currency,
            forecast: null,
            date: parseDate_1(event.date),
        };
    });
    const values_2 = eventsData_2.map(event => {
        return {
            title: event.title,
            impact: event.impact,
            previous: event.previous,
            currency: event.country,
            forecast: event.forecast,
            date: parseDate_2(event.date, event.time),
        };
    });
    const values = values_1.slice();
    values_2.forEach(value => {
        let isExist = false;
        values_1.every(oValue => {
            if (isSameObj(value, oValue)) {
                isExist = true;
                return false;
            }
            return true;
        });
        if (!isExist) values.push(value);
    });
    const dValues = values.map(value => {
        return [
            date.format(value.date, 'YYYY-MM-DD HH:mm:ss'),
            value.title, 
            value.currency,
            value.previous,
            value.forecast,
            value.impact
        ];
    });
    const ret = await addEventsData(
        dValues
    ).then(data => {
        return(data);
    }).catch(err => {
        console.log(err);
        return null;
    });
    if (typeof ret === 'number' && ret > 0) {
        console.log(`Number of events inserted: ${ret}`);
    } else {
        console.log('Failed');
    }
}

main()
    .then(() => process.exit(0))
    .catch((e) => {
        console.log(e);
        process.exit(1);
    });
