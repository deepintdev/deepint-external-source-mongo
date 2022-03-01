// Express application

"use strict";

import Express from "express";
import CookieParser from "cookie-parser";
import ExpressSwagger from "@asanrom/express-swagger-generator";
import FS from "fs";
import Path from "path";
import HTTP from "http";
import HTTPS from "https";
import { Config } from "./config";
import { Controller } from "./controller";

// Express async errors
require("express-async-errors");

/**
 * Web App
 */
export class Application {
    // Express App
    private application: Express.Express;

    constructor() {
        // Create and configure express aplication
        this.application = Express();

        // Middleware
        this.application.use(Express.json({ limit: "16mb" }));
        this.application.use(Express.urlencoded({ limit: "16mb", extended: true }));
        this.application.use(CookieParser());

        // Swagger
        if (Config.getInstance().apiDocs) {
            ExpressSwagger(this.application, {
                swaggerDefinition: {
                    info: {
                        description: 'API Documentation',
                        title: 'Swagger',
                        version: '1.0.0',
                    },
                    host: "localhost:" + Config.getInstance().httpPort,
                    basePath: '/',
                    produces: [
                        "application/json"
                    ],
                    schemes: [ 'http', 'https'],
                },
                basedir: __dirname, //app absolute path
                files: ['./controllers/**/*.js'] //Path to the API handle folder
            });
        }

        // Controllers
        try {
            this.registerControllers();
        } catch (ex) {
            console.error(ex);
            console.error("Could not register application controllers");
        }

        // Error handler
        this.application.use("*", this.errorHandler.bind(this));
    }

    public registerControllers() {
        const files = FS.readdirSync(Path.resolve(__dirname, "controllers"));
        for (const file of files) {
            if (file.endsWith(".js")) {
                try {
                    const controllerModule = require(Path.resolve(__dirname, "controllers", file));
                    for (const key of Object.keys(controllerModule)) {
                        const controller = controllerModule[key];
                        if (controller && controller.prototype && typeof controller.prototype.register === "function"){
                            const instance: Controller = new controller();
                            instance.register(this.application);
                            if (Config.getInstance().logDebug) {
                                console.log("Registered controller: " + key);
                            }
                        }
                    }
                } catch (ex) {
                    console.error(ex);
                }
            }
        }
    }

    /**
     * Handle client request
     * @param req Request
     * @param res Response
     */
    private handle(req: HTTP.IncomingMessage, res: HTTP.ServerResponse) {
        try {
            decodeURI(req.url);
        } catch (ex) {
            res.writeHead(400);
            res.end("Invalid requested URL.");
            return;
        }
        this.application(req, res);
    }

    /**
     * Starts the web application.
     */
    public start() {
        // HTTP
        const http = HTTP.createServer(this.handle.bind(this)).on("error", (e: any) => {
            if (e.code === "EADDRINUSE") {
                console.error(`[HTTP] [FATAL] [EADDRINUSE] Address is in use, cannot bind to port ${Config.getInstance().httpPort}`);
            }
        });
        http.listen(Config.getInstance().httpPort, () => {
            console.log(`[HTTP] Application listening on port ${Config.getInstance().httpPort}`);
        });

        // HTTPS
        if (Config.getInstance().sslEnabled) {
            const https = HTTPS.createServer({
                cert: FS.readFileSync(Config.getInstance().sslCert),
                key: FS.readFileSync(Config.getInstance().sslKey),
            }, this.handle.bind(this)).on("error", (e: any) => {
                if (e.code === "EADDRINUSE") {
                    console.error(`[HTTPS] [FATAL] [EADDRINUSE] Address is in use, cannot bind to port ${Config.getInstance().sslPort}`);
                }
            });

            https.listen(Config.getInstance().sslPort, () => {
                console.log(`[HTTPS] Application listening on port ${Config.getInstance().sslPort}`);
            });
        }
    }

    /**
    * Error handler. All requests that resulted in error go to this method.
    * @param error Error thrown.
    * @param request The request object.
    * @param response The response object.
    * @param next The callback.
    */
    private errorHandler(error: any, request: any, response: any, next) {
        if (error) {
            if (error instanceof SyntaxError) {
                response.status(400);
                response.send("Error in body: " + error.message);
                return;
            }

            console.error(error);
            response.status(500);
            response.send("An internal server error ocurred. Check console for details.");
            return;
        }
        next();
    }
}
