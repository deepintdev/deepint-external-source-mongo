// Main

"use strict";

import { Config } from './config';
import { CrashGuard } from './crash-guard';
import { Application } from "./app";
import { DataSource } from './source';

function main() {
    Config.getInstance();

    DataSource.getInstance().runUpdateService().catch(err => {
        console.error(err);
    });

    // Web app
    const app = new Application();
    app.start();

    CrashGuard.enable();
}

main();
