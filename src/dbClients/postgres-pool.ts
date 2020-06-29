import { environment } from './../environment';
import * as pg from 'pg';

export async function getPostgesPool() {
    const pool = new pg.Pool({
        user: environment.postGisDBCreds.user,
        host: environment.postGisDBCreds.host,
        database: environment.postGisDBCreds.database,
        password: environment.postGisDBCreds.password,
        port: environment.postGisDBCreds.port
    });

    await pool.connect();
    return pool;
}