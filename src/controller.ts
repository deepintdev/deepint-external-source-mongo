// Controller

"use strict";

import Express from "express";
import { Config } from "./config";
import { secureStringCompare } from "./utils/text";

export class Controller {
    public register(application: Express.Express) {
        throw new Error("Controller not implemented yet. Override the register method.");
    }

    public checkAuth(request: Express.Request): boolean {
        const pubKey = request.headers["x-public-key"] + "";
        const secretKey = request.headers["x-secret-key"] + "";

        if (!secureStringCompare(Config.getInstance().pubKey, pubKey)) {
            return false;
        }

        if (!secureStringCompare(Config.getInstance().secretKey, secretKey)) {
            return false;
        }

        return true;
    }
}
