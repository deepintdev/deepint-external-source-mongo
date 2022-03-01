// Deepint sources utils

"use strict";

import { Filter } from "mongodb";

const QUERY_TREE_MAX_DEPH = 4;
const QUERY_TREE_MAX_CHILDREN = 16;

export interface QueryTree {
    type: string;
    operation: string;
    left: number;
    right: string;
    children: QueryTree[];
}

export function sanitizeQueryTree(tree: any, depth?: number): QueryTree {
    depth = depth || 0;
    const sanitized: QueryTree = {
        type: "anyof",
        operation: "",
        left: -1,
        right: "",
        children: [],
    };

    if (typeof tree === "object") {
        let type = ("" + tree.type).toLowerCase();

        if (!["single", "one", "anyof", "allof", "not"].includes(type)) {
            type = "anyof";
        }

        sanitized.type = type;

        let operation = ("" + tree.operation).toLowerCase();

        if (!["null", "eq", "lt", "le", "lte", "gt", "ge", "gte", "cn", "cni", "sw", "swi", "ew", "ewi"].includes(operation)) {
            operation = "";
        }

        sanitized.operation = operation;

        let left = -1;
        if (typeof tree.left === "number") {
            left = Math.floor(tree.left);
        }

        sanitized.left = left;

        if (tree.right === null) {
            sanitized.right = null;
        } else {
            let right = "" + tree.right;

            if (right.length > 1024) {
                right = right.substr(0, 1024);
            }

            sanitized.right = right;
        }

        if (depth < QUERY_TREE_MAX_DEPH && (type in { anyof: 1, allof: 1, not: 1 }) && typeof tree.children === "object" && tree.children instanceof Array) {
            for (let i = 0; i < tree.children.length && i < QUERY_TREE_MAX_CHILDREN; i++) {
                sanitized.children.push(sanitizeQueryTree(tree.children[i], depth + 1));
            }
        }
    }

    return sanitized;
}

export type FeatureType = 'nominal' | 'text' | 'numeric' | 'logic' | 'date';

export type InstanceType = string | number | Date | boolean;

export interface Feature {
    index: number;
    type: FeatureType;
    name: string;
}

export function turnInto(data: any, type: FeatureType): InstanceType {
    if (data === null || data === undefined) {
        return null;
    }
    switch (type) {
    case "nominal":
        return ("" + data).substr(0, 255);
    case "date":
        try {
            const date = new Date(data);
            date.toISOString();
            return date;
        } catch (ex) {
            return new Date(0);
        }
    case "numeric":
    {
        const n = Number(data);
        if (isNaN(n)) {
            return null;
        }
        return n;
    }
    case "logic":
    {
        if (data === "true" || data === "1") {
            return true;
        }
        if (data === "false" || data === "0") {
            return false;
        }
        return !!data;
    }
    default:
        return "" + data;
    }
}

function negateMongoQuery(query: Filter<any>): Filter<any> {
    const keys = Object.keys(query);

    for (const key of keys) {
        if (!key.startsWith("$")) {
            query[key] = { $not: query[key] };
        } else if (key === "$and") {
            query.$or = query.$and.map(negateMongoQuery);
            delete query.$and;
        } else if (key === "$or") {
            query.$and = query.$or.map(negateMongoQuery);
            delete query.$or;
        } else if (key === "$not") {
            return query[key];
        }
    }

    return query;
}

function escapeRegExp(text) {
    return text.replace(/[-[\]{}()*+?.,\\^$|#\s]/g, "\\$&");
}

export function toMongoFilter(features: Feature[], query: QueryTree): Filter<any> {
    if (!query) {
        return {};
    }
    let filter: Filter<any> = Object.create(null);
    switch (query.type) {
    case "anyof":
        {
            filter.$or = [];
            for (const child of query.children) {
                filter.$or.push(toMongoFilter(features, child));
            }
            if (filter.$or.length === 0) {
                filter = {};
            }
        }
        break;
    case "allof":
        {
            filter.$and = [];
            for (const child of query.children) {
                filter.$and.push(toMongoFilter(features, child));
            }
            if (filter.$and.length === 0) {
                filter = {};
            }
        }
        break;
    case "not":
        {
            const nd: Filter<any>[] = [];
            for (const child of query.children) {
                nd.push(toMongoFilter(features, child));
            }
            if (nd.length > 0) {
                filter = negateMongoQuery({ $and: nd });
            } else {
                filter = {};
            }
        }
        break;
    default:
    {
        if (query.operation !== "null" && query.right === null) {
            return {};
        }

        const feature = features[query.left];

        if (!feature) {
            return {};
        }

        const cmp = turnInto(query.right, feature.type);

        switch (query.operation) {
        case "null":
            filter.$or = [
                { $exists: false },
                { $eq: null }
            ];
            break;
        case "eq":
            filter[feature.name] = {
                $eq: cmp,
            };
            break;
        case "lt":
            filter[feature.name] = {
                $lt: cmp,
            };
            break;
        case "le":
        case "lte":
            filter[feature.name] = {
                $lte: cmp,
            };
            break;
        case "gt":
            filter[feature.name] = {
                $gt: cmp,
            };
            break;
        case "ge":
        case "gte":
            filter[feature.name] = {
                $gte: cmp,
            };
            break;
        case "cn":
            filter[feature.name] = {
                $regex: new RegExp("" + escapeRegExp(cmp) + "", ""),
            };
            break;
        case "cni":
            filter[feature.name] = {
                $regex: new RegExp("" + escapeRegExp(cmp) + "", "i"),
            };
            break;
        case "sw":
            filter[feature.name] = {
                $regex: new RegExp("^" + escapeRegExp(cmp) + "", ""),
            };
            break;
        case "swi":
            filter[feature.name] = {
                $regex: new RegExp("^" + escapeRegExp(cmp) + "", "i"),
            };
            break;
        case "ew":
            filter[feature.name] = {
                $regex: new RegExp("" + escapeRegExp(cmp) + "$", ""),
            };
            break;
        case "ewi":
            filter[feature.name] = {
                $regex: new RegExp("" + escapeRegExp(cmp) + "$", "i"),
            };
            break;
        default:
            return {};
        }
    }
    }

    return filter;
}
