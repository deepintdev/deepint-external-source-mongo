// Source update

"use strict";

import Express from "express";
import { Controller } from "../controller";
import { DataSource } from "../source";

/**
 * Controller for updating the source
 */
export class StatusController extends Controller {
    public register(application: Express.Express) {
        application.post("/update/push", this.pushInstances.bind(this));
        application.post("/update/notice", this.noticeUpdate.bind(this));
    }

    /**
     * Push instances to the data source
     * @route POST /update/push
     * @group update - Source updating methods
     * @param {string} x-public-key.header.required - Source public key
     * @param {string} x-secret-key.header.required - Source secret key
     * @param {Array.<object>} instances.body - Instances - eg: [{"sepalLength": 5.1, "sepalWidth": 3.5, "petalLength": 1.4, "petalWidth": 0.2, "species": "setosa"}]
     * @returns {void} 200 - Success
     * @returns {void} 401 - Unauthorized
     */
    public async pushInstances(request: Express.Request, response: Express.Response) {
        if (!this.checkAuth(request)) {
            response.status(401);
            response.end();
            return;
        }

        const instances = DataSource.getInstance().sanitizeInstances(request.body || []);

        await DataSource.getInstance().pushInstances(instances);

        DataSource.getInstance().noticeUpdate();

        response.status(200)
        response.end();
    }

    /**
     * Notices Deep Intelligence of a source update. Call this method if you updated the mongo database without using this API.
     * @route POST /update/notice
     * @group update - Source updating methods
     * @param {string} x-public-key.header.required - Source public key
     * @param {string} x-secret-key.header.required - Source secret key
     * @returns {void} 200 - Success
     * @returns {void} 401 - Unauthorized
     */
    public noticeUpdate(request: Express.Request, response: Express.Response) {
        if (!this.checkAuth(request)) {
            response.status(401);
            response.end();
            return;
        }

        DataSource.getInstance().noticeUpdate();

        response.status(200)
        response.end();
    }
}
