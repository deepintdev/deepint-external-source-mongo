// Crash Guard

"use strict";

/**
 * Logs an application error.
 * @param error The uncaught exception.
 */
function logCrash(error: Error) {
    console.error(error);
    console.error("[UNCAUGHT EXCEPTION] " + error.name + ": " + error.message + "\n" + error.stack);
}

/**
 * Tries a function until it succeeds
 * @param func The function
 * @param delay The delay to retry
 * @param callback The success callback
 */
function tryFunc(func: () => Promise<any>, delay: number, callback: () => any) {
    try {
        func().then(callback).catch(function (ex) {
            console.error(ex);
            setTimeout(tryFunc.bind(null, func, delay, callback), delay);
        });
    } catch (ex) {
        console.error(ex);
        setTimeout(tryFunc.bind(null, func, delay, callback), delay);
    }
}

/**
 * Prevents the application from crashing.
 * Logs the uncaught exceptions.
 */
export class CrashGuard {
    /**
     * Enables the crashguard.
     */
    public static enable() {
        process.on("uncaughtException", logCrash);
    }

    /**
     * Disables the crashguard.
     */
    public static disable() {
        process.removeListener("uncaughtException", logCrash);
    }

    /**
     * Runs code until it succeeds. If an exception occurs it will re-run the code
     * @param fn The function to run
     * @param delay The retry delay
     * @returns A promise
     */
    public static async runUntilSuccess(fn: () => Promise<any>, delay?: number): Promise<void> {
        return new Promise<void>(function (resolve) {
            tryFunc(fn, delay || 5000, resolve);
        });
    }
}
