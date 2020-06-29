import chalk from 'chalk';

export function logCyan(data: any,) {
    console.log(chalk.bgCyan(JSON.stringify(data)));
}