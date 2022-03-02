// Status information

"use strict";

import Express from "express";
import { Config } from "../config";
import { Controller } from "../controller";
import { DataSource } from "../source";
import { QueryTree } from "../utils/deepint-sources";

/**
 * Controller for external source
 */
export class StatusController extends Controller {
    public register(application: Express.Express) {
        application.get("/", this.index.bind(this));

        application.post("/source/metadata", this.getMetadata.bind(this));
        application.post("/source/query", this.queryInstances.bind(this));
        application.post("/source/count", this.countInstances.bind(this));

        application.post("/source/nominal/values", this.getNominalValues.bind(this));
    }

    /**
     * @typedef IndexResponse
     * @property {string} public_key.required - Source public key
     */

    /**
     * Index
     * @route GET /
     * @group source - Source management
     * @param {string} x-public-key.header.required - Source public key
     * @param {string} x-secret-key.header.required - Source secret key
     * @returns {IndexResponse.model} 200 - Success
     * @returns {void} 401 - Unauthorized
     */
    public index(request: Express.Request, response: Express.Response) {
        if (!this.checkAuth(request)) {
            if (Config.getInstance().apiDocs) {
                return response.redirect("/api-docs");
            }
            response.status(401);
            response.end();
            return;
        }

        response.json({ public_key: Config.getInstance().pubKey });
    }

    /**
     * @typedef SourceFeature
     * @property {number} index.required - Feature index
     * @property {enum} type.required - Feature type - eg: NOMINAL,NUMERIC,BOOLEAN
     */

    /**
     * @typedef SourceMetadata
     * @property {string} id.required - Source ID
     * @property {Array.<SourceFeature>} features.required - Features
     * @property {number} count - Instances count
     */

    /**
     * Get source metadata
     * @route POST /source/metadata
     * @group source - Source management
     * @param {string} x-public-key.header.required - Source public key
     * @param {string} x-secret-key.header.required - Source secret key
     * @returns {SourceMetadata.model} 200 - Success
     * @returns {void} 401 - Unauthorized
     */
    public getMetadata(request: Express.Request, response: Express.Response) {
        if (!this.checkAuth(request)) {
            response.status(401);
            response.end();
            return;
        }

        response.json({
            id: Config.getInstance().pubKey,
            features: DataSource.getInstance().fields.map(a => {
                return {
                    index: a.index,
                    type: a.type.toUpperCase(),
                };
            }),
            count: DataSource.getInstance().countInstances(null),
        });
    }

    /**
     * @typedef QueryTree
     * @property {enum} type.required - Node type - eg: single,one,anyof,allof,not
     * @property {enum} operation - Operation type - eg: none,null,eq,lt,le,gt,ge,cn,cni,sw,swi,ew,ewi
     * @property {number} left - Index of the feature to compare
     * @property {string} right - Value to compare against
     * @property {Array.<QueryTree>} children - Children nodes, for types AnyOf, Not and AllOf
     */

    /**
     * @typedef QueryRequest
     * @property {QueryTree.model} filter.required - Filter to apply. Set null for no filter.
     * @property {string} projection - Projection, List of indexes split by commas
     * @property {number} order - Feature index to order by. Set to -1 for no order.
     * @property {enum} dir - Order direction - eg: asc,desc
     * @property {number} skip - Number of instances to skip
     * @property {number} limit - Max number of instances to return
     */

    /**
     * @typedef QueryResponse
     * @property {Array.<SourceFeature>} features.required - Features
     * @property {Array.<Array.<string>>} instances.required - Instances
     */

    /**
     * Query
     * @route POST /source/query
     * @group source - Source management
     * @param {string} x-public-key.header.required - Source public key
     * @param {string} x-secret-key.header.required - Source secret key
     * @param {QueryRequest.model} request.body - Request body
     * @returns {QueryResponse.model} 200 - Success
     * @returns {void} 401 - Unauthorized
     */
    public async queryInstances(request: Express.Request, response: Express.Response) {
        if (!this.checkAuth(request)) {
            response.status(401);
            response.end();
            return;
        }

        let filter: QueryTree;

        try {
            filter = DataSource.getInstance().sanitizeFilter(request.body.filter);
        } catch (ex) {
            response.status(400);
            response.send(ex.message);
            return;
        }

        const order = (request.body.order === undefined || request.body.order === "") ? -1 : (parseInt(request.body.order + "", 10) || 0);
        const dir = request.body.dir + "";

        const skip = parseInt(request.body.skip + "", 10) || 0;
        const limit = parseInt(request.body.limit + "", 10) || 0;

        const projection = DataSource.getInstance().sanitizeProjection((request.body.projection || "") + "");

        response.status(200);
        response.contentType("application/json");

        response.write(`{\n`);

        let requiresComma = false;

        if (Config.getInstance().logDebug) {
            console.log("[QUERY] [FILTER] " + JSON.stringify(filter));
        }

        await DataSource.getInstance().query(filter, order, dir, skip, limit, projection, (features) => {
            const featuresJSON = features.map(a => {
                return {
                    index: a.index,
                    type: a.type.toUpperCase(),
                };
            });

            response.write(`\t"features": ${JSON.stringify(featuresJSON)},\n`);
            response.write(`\t"instances": [\n`);
        }, (row) => {
            if (requiresComma) {
                response.write(`,\n`);
            }
            response.write(`\t${JSON.stringify(row)}`);
            requiresComma = true;
        });

        response.write(`]}`);
        response.end();
    }

    /**
     * @typedef CountRequest
     * @property {QueryTree.model} filter.required - Filter to apply. Set null for no filter.
     */

    /**
     * @typedef CountResponse
     * @property {number} count.required - Instances count
     */

    /**
     * Count
     * @route POST /source/count
     * @group source - Source management
     * @param {string} x-public-key.header.required - Source public key
     * @param {string} x-secret-key.header.required - Source secret key
     * @param {CountRequest.model} request.body - Request body
     * @returns {CountResponse.model} 200 - Success
     * @returns {void} 401 - Unauthorized
     */
    public async countInstances(request: Express.Request, response: Express.Response) {
        if (!this.checkAuth(request)) {
            response.status(401);
            response.end();
            return;
        }

        let filter: QueryTree;

        try {
            filter = DataSource.getInstance().sanitizeFilter(request.body.filter);
        } catch (ex) {
            response.status(400);
            response.send(ex.message);
            return;
        }

        const count = await DataSource.getInstance().countInstances(filter);

        response.json({
            count: count,
        })
    }

    /**
     * @typedef NominalRequest
     * @property {number} feature - Feature index
     * @property {QueryTree.model} filter - Filter to apply. Set null for no filter.
     * @property {string} query - Nominal string query
     */

    /**
     * Get nominal values
     * @route POST /source/nominal/values
     * @group source - Source management
     * @param {string} x-public-key.header.required - Source public key
     * @param {string} x-secret-key.header.required - Source secret key
     * @param {NominalRequest.model} request.body - Request body
     * @returns {Array.<string>} 200 - Success
     * @returns {void} 401 - Unauthorized
     */
    public async getNominalValues(request: Express.Request, response: Express.Response) {
        if (!this.checkAuth(request)) {
            response.status(401);
            response.end();
            return;
        }

        let filter: QueryTree;

        try {
            filter = DataSource.getInstance().sanitizeFilter(request.body.filter);
        } catch (ex) {
            response.status(400);
            response.send(ex.message);
            return;
        }

        const feature = request.body.feature === undefined ? -1 : (parseInt(request.body.feature + "", 10) || 0);
        const query = (request.body.query || "") + "";

        const result = await DataSource.getInstance().getNominalValues(filter, query, feature);

        response.json(result);
    }
}
