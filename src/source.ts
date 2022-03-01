// Source manager

"use strict";

import { FindCursor, MongoClient } from "mongodb";
import { Readable } from "stream";
import { Config } from "./config";
import { Feature, InstanceType, QueryTree, sanitizeQueryTree, toMongoFilter, turnInto } from "./utils/deepint-sources";

export class DataSource {
    public static instance: DataSource;

    public static getInstance() {
        if (DataSource.instance) {
            return DataSource.instance;
        }

        DataSource.instance = new DataSource();

        return DataSource.instance;
    }

    public fields: Feature[];
    public url: string;
    public mongoClient: MongoClient;

    constructor() {
        this.fields = Config.getInstance().sourceFeatures.map((f, i) => {
            return {
                index: i,
                name: f,
                type: Config.getInstance().sourceFeaturesTypes[i] || "text",
            };
        });

        this.url = Config.getInstance().mongoURI;
        this.mongoClient = new MongoClient(this.url, {
            forceServerObjectId: true,
        });
    }

    async connect(): Promise<MongoClient> {
        return this.mongoClient.connect();
    }

    public sanitizeFilter(json: any): QueryTree {
        if (!json) {
            return null;
        }
        return sanitizeQueryTree(json, 0);
    }

    public sanitizeProjection(projection: string): number[] {
        if (!projection) {
            return [];
        }

        return projection.split(",").map(a => {
            return parseInt(a, 10);
        }).filter(a => {
            if (isNaN(a) || a < 0) {
                return false;
            }
            return !!this.fields[a];
        });
    }

    /**
     * Counts instances
     * @param filter Filter to apply
     * @returns Instances count
     */
    public async countInstances(filter: QueryTree): Promise<number> {
        const cond1 = toMongoFilter(this.fields, filter);

        const client = await this.connect()
        const db = client.db().collection(Config.getInstance().mongoCollection);

        const cursor: FindCursor<any> = db.find(cond1);

        const count = await cursor.count();

        return count;
    }

    /**
     * Query instances
     * @param filter Filter to apply
     * @param order Feature to order by
     * @param dir Order direction
     * @param skip Instances to skip
     * @param limit Limit of instances to return
     * @param projection Projection to apply
     * @param onStart Called with the list of features
     * @param onRow Called for each row
     */
    public async query(filter: QueryTree, order: number, dir: string, skip: number, limit: number, projection: number[], onStart: (features: Feature[]) => void, onRow: (instance: InstanceType[]) => void): Promise<void> {
        let features = this.fields;

        let mongoProjection = null;
        let mongoSort = null;

        if (projection && projection.length > 0) {
            features = [];
            mongoProjection = Object.create(null);
            for (const f of projection) {
                if (this.fields[f]) {
                    mongoProjection[this.fields[f].name] = 1;
                    features.push(this.fields[f]);
                }
            }
        }

        if (order >= 0 && this.fields[order]) {
            mongoSort = Object.create(null);
            mongoSort[this.fields[order].name] = (dir === "desc" ? -1 : 1)
        }

        const cond1 = toMongoFilter(this.fields, filter);

        const client = await this.connect()
        const db = client.db().collection(Config.getInstance().mongoCollection);

        let cursor: FindCursor<any> = db.find(cond1);

        if (mongoSort) {
            cursor = cursor.sort(mongoSort);
        }

        if (mongoProjection) {
            cursor = cursor.project(mongoProjection);
        }

        if (skip !== null && skip > 0) {
            cursor = cursor.skip(skip);
        }

        if (limit !== null && limit > 0) {
            cursor = cursor.limit(limit);
        }

        return new Promise<void>((resolve, reject) => {
            const stream: Readable = cursor.stream();

            onStart(features);

            stream.on("data", (row) => {
                const instance = [];
                for (const feature of features) {
                    instance.push(turnInto(row[feature.name], feature.type));
                }
                onRow(instance)
            });

            stream.on("end", () => {
                resolve();
            });

            stream.on("error", (err) => {
                reject(err);
            });
        });
    }

    /**
     * Get nominal values
     * @param filter Filter to apply
     * @param query Text query for the field
     * @param feature Nominal feature
     * @returns List of nominal values
     */
    public async getNominalValues(filter: QueryTree, query: string, feature: number): Promise<string[]> {
        if (!this.fields[feature] || this.fields[feature].type !== 'nominal') {
            return [];
        }

        const cond1 = toMongoFilter(this.fields, filter);
        const fieldName = this.fields[feature].name;

        query = (query || "").toLowerCase();

        const mongoProjection = Object.create(null);
        mongoProjection[fieldName] = 1;

        const mongoSort = Object.create(null);
        mongoSort[fieldName] = 1;

        const client = await this.connect();

        const db = client.db().collection(Config.getInstance().mongoCollection);
        let cursor: FindCursor<any> = db.find(cond1);

        cursor = cursor.sort(mongoSort);
        cursor = cursor.project(mongoProjection);
        cursor = cursor.limit(128);

        const docs = await cursor.toArray();

        return docs.filter(doc => {
            return !!doc[fieldName];
        }).map(doc => {
            return (doc[fieldName] || "") + "";
        });
    }
}
