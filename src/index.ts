#!/usr/bin / env node

import * as pg from 'pg';
import * as newYork from './input/ny.json';
import * as kfcs from './input/kfcs.json';
import { getPostgresPool } from './dbClients/postgres-pool';
import { logGreen } from './logger';

main();

async function main() {
    const postgresPool = await getPostgresPool();
    await insertNYBoundary(postgresPool);
    await insertKFCBoundaries(postgresPool);
    logGreen('Execution complete');
}

async function insertNYBoundary(pgPool: pg.Pool) {
    const nyBoundaryTable = 'ny_boundary';
    const tableCreationQuery = buildTaleCreationQuery(nyBoundaryTable, pgPool);
    await pgPool.query(tableCreationQuery);

    const insertionQuery = buildInsertionQuery(JSON.stringify(newYork.geometry), nyBoundaryTable);
    await pgPool.query(insertionQuery);
}

async function insertKFCBoundaries(pgPool: pg.Pool) {
    const kfcBoundariesTable = 'kfc_boundaries';
    const tableCreationQuery = buildTaleCreationQuery(kfcBoundariesTable, pgPool);
    await pgPool.query(tableCreationQuery);

    const insertQueries = kfcs.features.map(kfc => buildInsertionQuery(JSON.stringify(kfc.geometry), kfcBoundariesTable));
    insertQueries.forEach(async insertQuery => await pgPool.query(insertQuery));
}

function buildTaleCreationQuery(tableName: String, pgPool: pg.Pool) {
    return 'create table if not exists ' + tableName + ' (id serial primary key, geom geometry)';
}

function buildInsertionQuery(data: any, table: String) {
    const queryPart1 = 'insert into ' + table + ' (geom) values (ST_GeomFromGeoJSON(\'';
    const queryPart3 = '\'));';
    const query = queryPart1.concat(data).concat(queryPart3);
    return query;
}