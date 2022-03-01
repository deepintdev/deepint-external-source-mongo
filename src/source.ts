// Source manager

"use strict";

import { AsyncSemaphore } from "@asanrom/async-tools";
import { FindCursor, MongoClient } from "mongodb";
import { Readable } from "stream";
import { Config } from "./config";
import { Feature, InstanceType, QueryTree, sanitizeQueryTree, toMongoFilter, turnInto } from "./utils/deepint-sources";
import { Request } from "./utils/request";

const DEEPINT_UPDATE_INSTANCES_LIMIT = 100;

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

    public updateSem: AsyncSemaphore;
    public updateQueue: InstanceType[][];
    public requiredUpdate: boolean;

    public closed: boolean;

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
        this.updateQueue = [];
        this.updateSem = new AsyncSemaphore(0);
        this.requiredUpdate = false;

        this.closed = false;
    }

    private async sendInstancesToDeepIntelligence(instances: InstanceType[][]): Promise<void> {
        const url = (new URL("external/source/update", Config.getInstance().deepintURL)).toString();
        return new Promise<void>((resolve, reject) => {
            Request.post(
                url,
                {
                    headers: {
                        'x-public-key': Config.getInstance().pubKey,
                        'x-secret-key': Config.getInstance().secretKey,
                    },
                    json: instances,
                },
                (err, response, body) => {
                    if (err) {
                        return reject(err);
                    }
                    if (response.statusCode !== 200) {
                        return reject(new Error("Status code: " + response.statusCode));
                    }
                    resolve();
                },
            )
        });
    }

    public async runUpdateService() {
        while (!this.closed) {
            await this.updateSem.acquire();

            if (!this.requiredUpdate && this.updateQueue.length === 0) {
                continue;
            }

            const instancesToPush: InstanceType[][] = [];

            while (instancesToPush.length < DEEPINT_UPDATE_INSTANCES_LIMIT && this.updateQueue.length > 0) {
                instancesToPush.push(this.updateQueue.shift());
            }

            this.requiredUpdate = false;

            let done = false;

            while(!done) {
                try {
                    await this.sendInstancesToDeepIntelligence(instancesToPush);
                    done = true;
                } catch (ex) {
                    console.error(ex);
                }

                if (!done) {
                    // If failure, wait 5 seconds to retry
                    await new Promise((resolve) => {
                        setTimeout(resolve, 5000);
                    });
                }
            }

            if (Config.getInstance().logEvents) {
                console.log(`[${(new Date()).toISOString()}] [UPDATE] External source updated.`);
            }
        }
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

    public sanitizeInstances(instances: any[]): InstanceType[][] {
        if (!Array.isArray(instances)) {
            return [];
        }
        return instances.map(i => {
            const instance: InstanceType[] = [];
            let row = i;
            if (typeof i !== "object") {
                row = Object.create(null);
            }

            for (const feature of this.fields) {
                instance.push(turnInto(row[feature.name], feature.type));
            }

            return instance;
        });
    }

    /**
     * Adds instances to the collection
     * @param instances Instances
     */
    public async pushInstances(instances: InstanceType[][]): Promise<void> {
        // Insert into mongo
        const mongoInstances = instances.map(i => {
            const row: any = Object.create(null);
            for (const feature of this.fields) {
                row[feature.name] = i[feature.index];
            }
            return row;
        });

        const client = await this.connect()
        const db = client.db().collection(Config.getInstance().mongoCollection);
        await db.insertMany(mongoInstances);

        // Add to queue
        instances.forEach(i => {
            this.updateQueue.push(i)
        });
    }

    /**
     * Notices a source update
     */
    public noticeUpdate() {
        this.requiredUpdate = true;
        this.updateSem.release();
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
