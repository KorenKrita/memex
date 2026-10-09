// Derived-work loop (DESIGN §5.5): one job at a time (server has little memory; concurrency is a deployment knob,
// not needed for correctness — every job publishes through finishJob).
import { claimJob, failJob, type Db, type Job } from "./db.ts";
import { runProject } from "./project.ts";

export type Handler = (db: Db, job: Job) => Promise<unknown>;
export const handlers: Record<string, Handler> = { project: runProject };

/** Run one due job of the given kinds. Returns false when none was due. */
export async function runOne(db: Db, kinds = Object.keys(handlers)): Promise<boolean> {
  const job = await claimJob(db, kinds);
  if (!job) return false;
  try {
    await handlers[job.kind](db, job);
  } catch (e) {
    console.error(`job ${job.kind} ${job.key.slice(0, 16)}: ${(e as Error).message?.slice(0, 300)}`);
    if (process.env.MEMEX_DEBUG) console.error((e as Error).stack);
    await failJob(db, job, e);
  }
  return true;
}

/** Run due jobs until none is left (tests, import). */
export async function drain(db: Db, kinds?: string[], max = 10_000) {
  let n = 0;
  while (n < max && (await runOne(db, kinds))) n++;
  return n;
}

export async function loop(db: Db, signal: AbortSignal, idleMs = 1000) {
  while (!signal.aborted) if (!(await runOne(db))) await Bun.sleep(idleMs);
}
