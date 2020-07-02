#!/usr/bin / env node

import * as pg from 'pg';
import * as newYork from './input/ny.json';
import * as kfcs from './input/kfcs.json';
import { getPostgresPool } from './dbClients/postgres-pool';
import { logGreen } from './logger';
import { writeFile } from './file-writer';

main();

async function main() {
    const postgresPool = await getPostgresPool();
    await insertKFCBoundaries(postgresPool);
    await insertNYBoundary(postgresPool);
    await expandBoundariesBy205Mtrs(postgresPool);
    await expandBoundariesBy300Mtrs(postgresPool);
    await subtractLevel1FromLevel2(postgresPool);
    await findIntersection(postgresPool);
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

async function expandBoundariesBy205Mtrs(pgPool: pg.Pool) {
    const level1Boundaries = 'level1_boundaries';
    const tableCreationQuery = buildTaleCreationQuery(level1Boundaries, pgPool);
    await pgPool.query(tableCreationQuery);

    const expansionQuery = buildExpansionQuery(205);
    const expandedBoundaryInsertionQuery = 'insert into ' + level1Boundaries + ' (geom) ' + expansionQuery;
    await pgPool.query(expandedBoundaryInsertionQuery);
}

async function expandBoundariesBy300Mtrs(pgPool: pg.Pool) {
    const level2Boundaries = 'level2_boundaries';
    const tableCreationQuery = buildTaleCreationQuery(level2Boundaries, pgPool);
    await pgPool.query(tableCreationQuery);

    const expansionQuery = buildExpansionQuery(300);
    const expandedBoundaryInsertionQuery = 'insert into ' + level2Boundaries + ' (geom) ' + expansionQuery;
    await pgPool.query(expandedBoundaryInsertionQuery);
}

async function subtractLevel1FromLevel2(pgPool: pg.Pool) {
    const boundaryDifference = 'boundary_difference';
    const tableCreationQuery = buildTaleCreationQuery(boundaryDifference, pgPool);
    await pgPool.query(tableCreationQuery);

    const level1 = (await pgPool.query('select geom from level1_boundaries')).rows[0].geom;
    const level2 = (await pgPool.query('select geom from level2_boundaries')).rows[0].geom;
    const query = "insert into " + boundaryDifference + " (geom) select ST_Difference(\'" + level2 + "\',\'" + level1 + "\');";
    await pgPool.query(query);
}

async function findIntersection(pgPool: pg.Pool) {
    const ny = (await pgPool.query('select geom from ny_boundary')).rows[0].geom;
    const difference = (await pgPool.query('select geom from boundary_difference')).rows[0].geom;
    const query = "select st_asgeojson(ST_Intersection(\'" + difference + "\',\'" + ny + "\'));";
    const newLocal = await pgPool.query(query);
    logGreen(JSON.stringify(newLocal.rows[0].st_asgeojson));
    writeFile('green_zones', JSON.stringify(newLocal.rows[0].st_asgeojson));
}

function buildExpansionQuery(distanceInMeters: number) {
    return 'select st_union(array (select st_buffer(geom::geography, ' + distanceInMeters + ' )::geometry from kfc_boundaries))'
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