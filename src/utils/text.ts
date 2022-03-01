// Text utils

"use strict";

import Crypto from "crypto";

export function secureStringCompare(a: string, b: string): boolean {
    try {
        return Crypto.timingSafeEqual(Buffer.from(a, 'utf8'), Buffer.from(b, 'utf8'));
    } catch (ex) {
        return false;
    }
}
