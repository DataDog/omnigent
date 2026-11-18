// Create the two spike users: alice (first-run admin) and bob (invited).
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { randomBytes } from "node:crypto";
import { Api } from "./api.ts";
import { RUN_DIR, loginAs, serverBaseUrl, writeUsers } from "./env.ts";

const alicePassword = readFileSync(join(RUN_DIR, "admin_password"), "utf-8").trim();
const bobPassword = randomBytes(12).toString("hex");
writeUsers({ alice: { password: alicePassword }, bob: { password: bobPassword } });
const alice = await loginAs("alice");
const invite = await alice.invite();
console.log("invite", invite.status, Object.keys(invite.body ?? {}));
const url = String(invite.body.register_url ?? invite.body.url ?? "");
const token = invite.body.token ?? new URL(url).searchParams.get("invite") ?? url.split("/").pop();
const register = await new Api(serverBaseUrl()).register(token, "bob", bobPassword);
console.log("register bob", register.status, Object.keys(register.body ?? {}));
