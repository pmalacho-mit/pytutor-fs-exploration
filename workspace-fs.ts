/**
 * workspace-fs.ts  (v2 — backend-fed tree metadata)
 *
 * Change from v1: the workspace tree is NO LONGER a yjs doc in a Liveblocks
 * meta room. Postgres is the single source of truth, and clients receive the
 * tree through your backend directly:
 *
 *   1. Snapshot   GET /api/workspaces/{id}/tree
 *                   -> { seq: number, entries: FileMeta[] }
 *   2. Feed (SSE) GET /api/workspaces/{id}/tree/feed?after={seq}
 *                   event: "tree"   data: { seq, fileId, fields: Partial<FileMetaFields> }
 *                   event: "reset"  data: {}     // client too far behind -> re-snapshot
 *                   (server sends comment heartbeats so proxies keep the stream open)
 *
 * Server contract (the part that makes this design sound):
 *   - each workspace has a monotonic `tree_seq` in Postgres
 *   - every tree/content mutation bumps it AND emits its feed event derived
 *     from the same transaction — the feed is generated FROM the truth, so
 *     there is no dual-write drift by construction
 *   - the feed retains a bounded event window; resume beyond it -> "reset"
 *
 * Client rules:
 *   - apply events strictly in order; any gap (event.seq !== seq + 1) -> re-snapshot
 *   - on reconnect, resume with ?after={seq}; on any doubt, re-snapshot
 *     (the tree is small — boring-and-correct beats clever)
 *   - persisted cache is plain JSON { seq, entries } in IndexedDB -> offline tree
 *
 * UNCHANGED from v1 (deliberately — everything downstream consumes the same
 * WorkspaceFS interface): per-file yjs docs in per-file Liveblocks rooms,
 * y-indexeddb persistence, refcounted attach/detach, the ContentCache, and
 * the readFile / writeTextFile router functions.
 */

import * as Y from "yjs";
import { Client } from "@liveblocks/client";
import { LiveblocksYjsProvider } from "@liveblocks/yjs";
import { IndexeddbPersistence } from "y-indexeddb";

// ---------------------------------------------------------------------------
// Shared types & conventions
// ---------------------------------------------------------------------------

export type EntryType = "file" | "folder";
export type FileKind = "text" | "text_blob" | "binary";

export interface FileMetaFields {
  name: string;
  parentId: string | null; // null = workspace root
  type: EntryType;
  deleted: boolean;
  // file-only fields (absent on folders):
  kind?: FileKind;
  mime?: string;
  size?: number;
  version?: number; // monotonic content version, bumped by backend on commit
  blobHash?: string; // set when kind is "binary" | "text_blob"
}

export type FileMeta = FileMetaFields & { id: string };

export interface TreeSnapshot {
  seq: number;
  entries: FileMeta[];
}

/**
 * One feed event. `fields` is a partial merge into the entry (a create event
 * simply carries all fields; a delete carries { deleted: true }).
 */
export interface TreeEvent {
  seq: number;
  fileId: string;
  fields: Partial<FileMetaFields>;
}

const FILE_ROOM = (fileId: string) => `file:${fileId}`;
const TEXT_KEY = "content";
const IDB_FILE_DOC = (fileId: string) => `ydoc:${fileId}`;
const IDB_LAST_SYNCED_SV = "lastSyncedStateVector";

// ---------------------------------------------------------------------------
// Tree transport — snapshot fetch + SSE feed (injectable for testing)
// ---------------------------------------------------------------------------

export interface FeedHandlers {
  onEvent(e: TreeEvent): void;
  onReset(): void;
  onStatus(connected: boolean): void;
}

export interface TreeTransport {
  fetchSnapshot(workspaceId: string): Promise<TreeSnapshot>;
  /**
   * Open the change feed. `after` is a getter so reconnects resume from the
   * client's CURRENT seq, not the seq at first subscription.
   * Returns a function that closes the feed permanently.
   */
  openFeed(workspaceId: string, after: () => number, handlers: FeedHandlers): () => void;
}

export class SseTreeTransport implements TreeTransport {
  constructor(private baseUrl = "/api") {}

  async fetchSnapshot(workspaceId: string): Promise<TreeSnapshot> {
    const res = await fetch(`${this.baseUrl}/workspaces/${workspaceId}/tree`, {
      credentials: "include",
    });
    if (!res.ok) throw new Error(`tree snapshot failed: ${res.status}`);
    return res.json();
  }

  openFeed(workspaceId: string, after: () => number, handlers: FeedHandlers): () => void {
    let source: EventSource | null = null;
    let closed = false;
    let retryMs = 1_000;

    const connect = () => {
      if (closed) return;
      source = new EventSource(
        `${this.baseUrl}/workspaces/${workspaceId}/tree/feed?after=${after()}`,
        { withCredentials: true }
      );
      source.onopen = () => {
        retryMs = 1_000;
        handlers.onStatus(true);
      };
      source.addEventListener("tree", (ev) => {
        handlers.onEvent(JSON.parse((ev as MessageEvent).data) as TreeEvent);
      });
      source.addEventListener("reset", () => handlers.onReset());
      // Manual reconnection: EventSource's built-in retry would reuse the
      // original URL and therefore a stale ?after=. We close and reopen so
      // resume always starts from the current seq.
      source.onerror = () => {
        handlers.onStatus(false);
        source?.close();
        const jitter = Math.random() * 0.3 + 0.85;
        setTimeout(connect, retryMs * jitter);
        retryMs = Math.min(retryMs * 2, 30_000);
      };
    };

    connect();
    return () => {
      closed = true;
      source?.close();
    };
  }
}

// ---------------------------------------------------------------------------
// Small promise-based IndexedDB KV helper (used by tree cache + content cache)
// ---------------------------------------------------------------------------

class KvDb {
  private dbPromise: Promise<IDBDatabase>;

  constructor(name: string, private stores: string[]) {
    this.dbPromise = new Promise((resolve, reject) => {
      const req = indexedDB.open(name, 1);
      req.onupgradeneeded = () => {
        for (const s of stores) req.result.createObjectStore(s);
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
  }

  async get<T>(store: string, key: IDBValidKey): Promise<T | undefined> {
    const db = await this.dbPromise;
    return new Promise((resolve, reject) => {
      const req = db.transaction(store, "readonly").objectStore(store).get(key);
      req.onsuccess = () => resolve(req.result as T | undefined);
      req.onerror = () => reject(req.error);
    });
  }

  async put(store: string, key: IDBValidKey, value: unknown): Promise<void> {
    const db = await this.dbPromise;
    return new Promise((resolve, reject) => {
      const req = db.transaction(store, "readwrite").objectStore(store).put(value, key);
      req.onsuccess = () => resolve();
      req.onerror = () => reject(req.error);
    });
  }
}

// ---------------------------------------------------------------------------
// WorkspaceFS — backend-fed tree with local JSON cache
// ---------------------------------------------------------------------------

export type TreeStatus =
  | "empty"   // no cache, no network yet — tree unavailable
  | "cached"  // serving last-known-good from IndexedDB, feed not (yet) live
  | "live";   // snapshot applied and feed connected

export class WorkspaceFS {
  private byId = new Map<string, FileMeta>();
  private childIndex = new Map<string | null, Map<string, string>>(); // parentId -> (name -> id)
  private seq = -1;
  private _status: TreeStatus = "empty";
  private listeners = new Set<() => void>();
  private closeFeed: (() => void) | null = null;
  private persistTimer: ReturnType<typeof setTimeout> | null = null;
  private resyncInFlight = false;

  private constructor(
    private transport: TreeTransport,
    private workspaceId: string,
    private db: KvDb
  ) {}

  /**
   * Open the workspace tree. Cache-first for instant/offline availability,
   * then snapshot + feed in the background. Never throws on network failure —
   * status tells you what you're looking at.
   */
  static async open(transport: TreeTransport, workspaceId: string): Promise<WorkspaceFS> {
    const db = new KvDb(`tree-cache:${workspaceId}`, ["tree"]);
    const ws = new WorkspaceFS(transport, workspaceId, db);

    // 1. Last-known-good from IndexedDB — tree renders instantly, works offline.
    const cached = await db.get<TreeSnapshot>("tree", "snapshot");
    if (cached) {
      ws.applySnapshot(cached, "cached");
    }

    // 2. Authoritative snapshot + live feed, in the background.
    void ws.resync();
    ws.closeFeed = transport.openFeed(workspaceId, () => ws.seq, {
      onEvent: (e) => ws.handleEvent(e),
      onReset: () => void ws.resync(),
      onStatus: (connected) => {
        if (connected) {
          // Resumed with ?after=seq; events now bridge any gap. A resync here
          // would also be correct — resume is just cheaper for short blips.
        } else if (ws._status === "live") {
          ws.setStatus("cached"); // be honest: we may now be stale
        }
      },
    });

    return ws;
  }

  // -- tree queries (interface unchanged from v1) ---------------------------

  getMeta(fileId: string): FileMeta | undefined {
    return this.byId.get(fileId);
  }

  /** Resolve an absolute path ("/src/main.py") to an entry id, or null. */
  resolve(path: string): string | null {
    const segments = path.split("/").filter(Boolean);
    let parent: string | null = null;
    let currentId: string | null = null;
    for (const name of segments) {
      const id = this.childIndex.get(parent)?.get(name);
      if (!id) return null;
      currentId = id;
      parent = id;
    }
    return currentId;
  }

  /** List non-deleted children of a folder (null = root). */
  list(parentId: string | null): FileMeta[] {
    const siblings = this.childIndex.get(parentId);
    if (!siblings) return [];
    return [...siblings.values()]
      .map((id) => this.byId.get(id)!)
      .sort((a, b) => a.name.localeCompare(b.name));
  }

  onChange(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  get status(): TreeStatus {
    return this._status;
  }

  /** Highest tree_seq applied — expose in UI as "synced to #N / N min ago". */
  get treeSeq(): number {
    return this.seq;
  }

  close(): void {
    this.closeFeed?.();
    if (this.persistTimer) clearTimeout(this.persistTimer);
  }

  // -- feed handling ---------------------------------------------------------

  private handleEvent(e: TreeEvent): void {
    // Strict ordering: any gap means we missed something — fall back to
    // the boring, correct answer and re-snapshot.
    if (e.seq !== this.seq + 1) {
      if (e.seq <= this.seq) return; // duplicate/old event: ignore
      void this.resync();
      return;
    }
    const existing = this.byId.get(e.fileId);
    const merged = { ...(existing ?? {}), ...e.fields, id: e.fileId } as FileMeta;
    this.byId.set(e.fileId, merged);
    this.seq = e.seq;
    this.rebuildChildIndex();
    this.schedulePersist();
    this.notify();
  }

  private async resync(): Promise<void> {
    if (this.resyncInFlight) return;
    this.resyncInFlight = true;
    try {
      const snapshot = await this.transport.fetchSnapshot(this.workspaceId);
      if (snapshot.seq >= this.seq) this.applySnapshot(snapshot, "live");
    } catch {
      // Offline / backend down: keep serving cached state; feed's backoff
      // loop (or the next reset) will bring us here again.
    } finally {
      this.resyncInFlight = false;
    }
  }

  private applySnapshot(s: TreeSnapshot, status: TreeStatus): void {
    this.byId = new Map(s.entries.map((e) => [e.id, e]));
    this.seq = s.seq;
    this.rebuildChildIndex();
    this.setStatus(status);
    this.schedulePersist();
    this.notify();
  }

  private rebuildChildIndex(): void {
    this.childIndex.clear();
    for (const meta of this.byId.values()) {
      if (meta.deleted) continue;
      let siblings = this.childIndex.get(meta.parentId);
      if (!siblings) this.childIndex.set(meta.parentId, (siblings = new Map()));
      siblings.set(meta.name, meta.id);
    }
  }

  private schedulePersist(): void {
    if (this.persistTimer) return;
    this.persistTimer = setTimeout(() => {
      this.persistTimer = null;
      const snapshot: TreeSnapshot = { seq: this.seq, entries: [...this.byId.values()] };
      void this.db.put("tree", "snapshot", snapshot);
    }, 250);
  }

  private setStatus(s: TreeStatus): void {
    if (this._status !== s) {
      this._status = s;
      this.notify();
    }
  }

  private notify(): void {
    this.listeners.forEach((l) => l());
  }
}

// ---------------------------------------------------------------------------
// TextDocRegistry — refcounted per-file yjs docs/rooms (unchanged from v1)
// ---------------------------------------------------------------------------

function hasUnsyncedChanges(doc: Y.Doc, lastSyncedSV: Uint8Array | null): boolean {
  if (!lastSyncedSV) return true;
  // An empty yjs diff-update encodes to 2 bytes (0 structs + empty delete set).
  return Y.encodeStateAsUpdate(doc, lastSyncedSV).length > 2;
}

function providerSynced(provider: LiveblocksYjsProvider): Promise<void> {
  return new Promise((resolve) => {
    if (provider.synced) return resolve();
    const cb = (synced: boolean) => {
      if (synced) {
        provider.off("sync", cb);
        resolve();
      }
    };
    provider.on("sync", cb);
  });
}

interface OpenDoc {
  doc: Y.Doc;
  text: Y.Text;
  idb: IndexeddbPersistence;
  provider: LiveblocksYjsProvider;
  leave: () => void;
  refs: number;
}

export interface TextHandle {
  text: Y.Text;
  doc: Y.Doc;
  /** Resolves once local IndexedDB state is loaded (network may still be syncing). */
  localReady: Promise<void>;
  release: () => Promise<void>;
}

export class TextDocRegistry {
  private open = new Map<string, OpenDoc>();

  constructor(private client: Client) {}

  isOpen(fileId: string): boolean {
    return this.open.has(fileId);
  }

  currentContent(fileId: string): string | undefined {
    return this.open.get(fileId)?.text.toString();
  }

  async acquire(fileId: string): Promise<TextHandle> {
    let entry = this.open.get(fileId);

    if (!entry) {
      const doc = new Y.Doc({ guid: fileId });
      const text = doc.getText(TEXT_KEY);

      // Local persistence FIRST (instant state incl. prior offline edits),
      // then the network provider; CRDT merge reconciles the two.
      const idb = new IndexeddbPersistence(IDB_FILE_DOC(fileId), doc);
      const localReady = idb.whenSynced.then(() => undefined);

      const { room, leave } = this.client.enterRoom(FILE_ROOM(fileId));
      const provider = new LiveblocksYjsProvider(room, doc);

      provider.on("sync", (synced: boolean) => {
        if (synced) void idb.set(IDB_LAST_SYNCED_SV, Y.encodeStateVector(doc));
      });

      entry = { doc, text, idb, provider, leave, refs: 0 };
      this.open.set(fileId, entry);
      await localReady;
    }

    entry.refs++;
    const localReady = entry.idb.whenSynced.then(() => undefined);

    return {
      text: entry.text,
      doc: entry.doc,
      localReady,
      release: () => this.release(fileId),
    };
  }

  private async release(fileId: string): Promise<void> {
    const entry = this.open.get(fileId);
    if (!entry) return;
    entry.refs--;
    if (entry.refs > 0) return;

    // "Detach" means leave the room — never "forget unsynced work".
    const lastSV = ((await entry.idb.get(IDB_LAST_SYNCED_SV)) ?? null) as Uint8Array | null;
    if (hasUnsyncedChanges(entry.doc, lastSV)) {
      void providerSynced(entry.provider).then(() => {
        if (this.open.get(fileId)?.refs === 0) this.teardown(fileId, false);
      });
      return;
    }
    this.teardown(fileId, false);
  }

  /** File deleted or demoted to text_blob: drop doc AND wipe its local data. */
  async evict(fileId: string): Promise<void> {
    if (this.open.has(fileId)) {
      this.teardown(fileId, true);
    } else {
      const idb = new IndexeddbPersistence(IDB_FILE_DOC(fileId), new Y.Doc());
      await idb.whenSynced;
      await idb.clearData();
    }
  }

  private teardown(fileId: string, wipeLocal: boolean): void {
    const entry = this.open.get(fileId);
    if (!entry) return;
    this.open.delete(fileId);
    entry.provider.destroy();
    entry.leave();
    if (wipeLocal) void entry.idb.clearData();
    else entry.idb.destroy(); // keep data: warm cache for next open
    entry.doc.destroy();
  }
}

// ---------------------------------------------------------------------------
// ContentCache — version-token cache for non-live content (unchanged from v1)
// ---------------------------------------------------------------------------
//
//   "text"  : key = `${fileId}@${version}` — valid only for that exact version
//   "blobs" : key = blobHash              — immutable, valid forever
//
export class ContentCache {
  private db: KvDb;

  constructor(workspaceId: string) {
    this.db = new KvDb(`content-cache:${workspaceId}`, ["text", "blobs"]);
  }

  getText(fileId: string, version: number): Promise<Uint8Array | undefined> {
    return this.db.get("text", `${fileId}@${version}`);
  }
  putText(fileId: string, version: number, bytes: Uint8Array): Promise<void> {
    return this.db.put("text", `${fileId}@${version}`, bytes);
  }
  getBlob(hash: string): Promise<Uint8Array | undefined> {
    return this.db.get("blobs", hash);
  }
  putBlob(hash: string, bytes: Uint8Array): Promise<void> {
    return this.db.put("blobs", hash, bytes);
  }
}

// ---------------------------------------------------------------------------
// FS-router read/write paths (unchanged from v1)
// ---------------------------------------------------------------------------

export interface ContentApi {
  /** Backend plays back deltas (with snapshot checkpoints) to a full string. */
  materializeText(fileId: string): Promise<Uint8Array>;
  /** Fetch an immutable blob by content hash (presigned URL or proxy). */
  getBlob(hash: string): Promise<Uint8Array>;
  /** Upload bytes for `hash` unless the backend already has them (dedup). */
  uploadBlobIfMissing(hash: string, bytes: Uint8Array): Promise<void>;
  /**
   * Point the file at `hash`. Compare-and-swap on `expectedVersion`: the
   * backend rejects if someone committed in between (surface as a conflict /
   * conflicted copy — never silent last-write-wins). The backend may also
   * reclassify `kind` during this commit (e.g. text_blob -> binary when the
   * new bytes aren't valid UTF-8); the change flows back via the feed.
   */
  commitBlobPointer(fileId: string, hash: string, expectedVersion: number): Promise<void>;
}

const enc = new TextEncoder();

export async function readFile(
  ws: WorkspaceFS,
  registry: TextDocRegistry,
  cache: ContentCache,
  api: ContentApi,
  path: string
): Promise<Uint8Array> {
  const fileId = ws.resolve(path);
  const meta = fileId ? ws.getMeta(fileId) : undefined;
  if (!fileId || !meta || meta.deleted || meta.type !== "file") {
    throw new FsError("ENOENT", path);
  }

  // Case 1: text file with a live binding on this client -> serve the yjs doc.
  if (meta.kind === "text") {
    const live = registry.currentContent(fileId);
    if (live !== undefined) return enc.encode(live);

    // Case 2/3 for text: version-keyed cache, then backend materialization.
    const version = meta.version ?? 0;
    const cached = await cache.getText(fileId, version);
    if (cached) return cached;
    const bytes = await withDeadline(api.materializeText(fileId), 15_000, path);
    await cache.putText(fileId, version, bytes);
    return bytes;
  }

  // binary | text_blob: immutable content keyed by hash — a cache hit is
  // valid forever; only the pointer (meta.blobHash) can go stale, and the
  // feed keeps the pointer fresh.
  if (!meta.blobHash) throw new FsError("EIO", path);
  const cached = await cache.getBlob(meta.blobHash);
  if (cached) return cached;
  const bytes = await withDeadline(api.getBlob(meta.blobHash), 30_000, path);
  await cache.putBlob(meta.blobHash, bytes);
  return bytes;
}

/**
 * Kind-aware write router — what the Pyodide bridge should call. A file's
 * kind can CHANGE over its lifetime (text -> text_blob demotion, promotion
 * back, content-driven reclassification), so the route is decided per-write
 * from CURRENT metadata, never cached.
 */
export async function writeFile(
  ws: WorkspaceFS,
  registry: TextDocRegistry,
  cache: ContentCache,
  api: ContentApi,
  path: string,
  bytes: Uint8Array
): Promise<void> {
  const fileId = ws.resolve(path);
  const meta = fileId ? ws.getMeta(fileId) : undefined;
  if (!fileId || !meta || meta.deleted || meta.type !== "file") {
    throw new FsError("ENOENT", path);
  }

  if (meta.kind === "text") {
    try {
      const content = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
      return await writeTextFile(registry, fileId, content);
    } catch {
      // Bytes are no longer valid UTF-8: this write forces text -> binary.
      // Route to the blob path; the backend reclassifies kind on commit and
      // the change comes back via the feed (evicting the yjs doc — see wiring).
      return blobWrite(cache, api, meta, bytes);
    }
  }
  return blobWrite(cache, api, meta, bytes);
}

async function blobWrite(
  cache: ContentCache,
  api: ContentApi,
  meta: FileMeta,
  bytes: Uint8Array
): Promise<void> {
  const hash = await sha256Hex(bytes);
  await cache.putBlob(hash, bytes); // read-your-own-writes before any network
  await api.uploadBlobIfMissing(hash, bytes);
  await api.commitBlobPointer(meta.id, hash, meta.version ?? 0); // CAS
  // New version/hash/kind arrive via the feed; the byte cache is already warm.
}

async function sha256Hex(bytes: Uint8Array): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

/**
 * Programmatic text write for kind === "text" ONLY — call via writeFile().
 * ALL text mutations flow through yjs — never a parallel write path.
 */
export async function writeTextFile(
  registry: TextDocRegistry,
  fileId: string,
  newContent: string
): Promise<void> {
  const handle = await registry.acquire(fileId); // transient attach if not open
  try {
    // Best effort: converge before diffing so we diff against current truth.
    // Offline, proceed against local state; CRDT merge reconciles later.
    await Promise.race([handle.localReady, new Promise((r) => setTimeout(r, 5_000))]);
    handle.doc.transact(() => applyMinimalDiff(handle.text, newContent));
  } finally {
    await handle.release(); // flush-before-teardown handled by the registry
  }
}

/** Replace Y.Text content via common prefix/suffix diff (one delete + one insert). */
function applyMinimalDiff(ytext: Y.Text, next: string): void {
  const prev = ytext.toString();
  if (prev === next) return;
  let start = 0;
  const minLen = Math.min(prev.length, next.length);
  while (start < minLen && prev[start] === next[start]) start++;
  let endPrev = prev.length;
  let endNext = next.length;
  while (endPrev > start && endNext > start && prev[endPrev - 1] === next[endNext - 1]) {
    endPrev--;
    endNext--;
  }
  if (endPrev > start) ytext.delete(start, endPrev - start);
  if (endNext > start) ytext.insert(start, next.slice(start, endNext));
}

// ---------------------------------------------------------------------------
// Errors & deadlines — a hung fetch must NEVER wedge the Atomics-blocked worker
// ---------------------------------------------------------------------------

export class FsError extends Error {
  constructor(public code: "ENOENT" | "EIO" | "ETIMEDOUT", path: string) {
    super(`${code}: ${path}`);
  }
}

function withDeadline<T>(p: Promise<T>, ms: number, path: string): Promise<T> {
  return Promise.race([
    p,
    new Promise<never>((_, rej) => setTimeout(() => rej(new FsError("ETIMEDOUT", path)), ms)),
  ]);
}

// ---------------------------------------------------------------------------
// Wiring it together (illustrative)
// ---------------------------------------------------------------------------
//
// // Liveblocks is now used ONLY for per-file text collaboration:
// const lbClient = createClient({ authEndpoint: "/api/liveblocks-auth" });
// const registry = new TextDocRegistry(lbClient);
//
// // The tree comes straight from your backend:
// const ws = await WorkspaceFS.open(new SseTreeTransport("/api"), workspaceId);
// const cache = new ContentCache(workspaceId);
//
// ws.onChange(() => {
//   renderTree(ws.list(null), ws.status, ws.treeSeq);
//   // Kind transitions & deletes: a file demoted away from "text" must drop
//   // its yjs doc — and evict() wipes its y-indexeddb, because stale CRDT
//   // state must never resurrect into a future promotion's fresh doc. If
//   // this client holds unsynced local edits, salvage them as a blob write
//   // first; a CAS conflict there becomes a conflicted copy, never silent loss.
//   for (const id of knownOpenIds()) {
//     const m = ws.getMeta(id);
//     if (m && !m.deleted && m.kind === "text") continue;
//     const local = registry.currentContent(id);
//     if (m && !m.deleted && local !== undefined && hadUnsyncedEdits(id)) {
//       void writeFile(ws, registry, cache, api, pathOf(id), enc.encode(local));
//     }
//     void registry.evict(id);
//   }
// });
//
// // Editor opens a tab:
// const handle = await registry.acquire(fileId);
// await handle.localReady;
// bindCodeMirror(editorView, handle.text);
// // ...tab closes:
// await handle.release();
//
// // Pyodide bridge, main-thread side:
// const bytes = await readFile(ws, registry, cache, api, "/src/main.py");
//
// // Tree mutations (create/rename/move/delete) go through your HTTP API and
// // come back through the feed — the client never mutates tree state locally
// // in the online design. Your offline outbox/overlay wraps ws.getMeta/resolve.
