import {agreedEula} from "./Settings.ts";
import {
    BehaviorSubject,
    combineLatest,
    distinctUntilChanged,
    filter,
    from,
    map,
    Observable,
    shareReplay,
    switchMap,
    tap
} from "rxjs";
import {cachedFetch, downloadProgress, getJson, type TargetJar} from "./JarProvider.ts";
import {type Jar, openJar} from "../utils/Jar.ts";
import type {JarEntryPath} from "../utils/Names.ts";
// @ts-ignore
import { loadBspatch } from "bsdiff-wasm";

const VERSIONS_URL = "https://fill.papermc.io/v3/projects/paper/versions"
const LATEST_URL = "https://fill.papermc.io/v3/projects/paper/versions/%version%/builds/latest"

type PaperJar = TargetJar<PaperJarMetadata>;

export interface PaperJarMetadata {
    sha256: string;
}

interface VersionsList {
    versions: VersionListEntry[]
}

interface VersionListEntry {
    version: {
        id: string;
        support: {
            status: "SUPPORTED" | "UNSUPPORTED";
            end?: string;
        },
    }
}

interface VersionManifest {
    id: number;
    time: string;
    channel: string;
    downloads: {
        "server:default": {
            name: string;
            checksums: {
                "sha256": string;
            }
            size: number;
            url: string;
        }
    }
}

export const paperVersions = agreedEula.observable.pipe(
    filter(agreed => agreed),
    switchMap(() => from(fetchVersions())),
    shareReplay({bufferSize: 1, refCount: false})
);

async function fetchVersions(): Promise<VersionListEntry[]> {
    const fill = await getJson<VersionsList>(VERSIONS_URL);
    // TODO filter out unsupported
    return fill.versions.map(version => ({version: {...version.version, id: "paper-" + version.version.id}}));
}

export function paperJarPipeline(source$: Observable<string | null>): Observable<PaperJar> {
    return combineLatest([
        source$.pipe(
            filter(id => id !== null),
            distinctUntilChanged()
        ),
        paperVersions
    ]).pipe(
        map(([version, versions]) => versions.find(v => v.version.id === version)),
        filter((version) => version !== undefined),
        tap((version) => console.log(`Opening Paper jar ${version.version.id}`)),
        switchMap(version => from(downloadPaperJar(version, downloadProgress))),
        shareReplay({bufferSize: 1, refCount: false})
    );
}


async function downloadPaperJar(version: VersionListEntry, progress: BehaviorSubject<number | undefined>): Promise<PaperJar> {
    const cleanedId = version.version.id.replace("paper-", "");
    console.log(`Downloading latest Paper jar for version: ${cleanedId}`);
    const versionManifest = await getJson<VersionManifest>(LATEST_URL.replace("%version%", cleanedId))

    let rawBlob: Blob;

    try {
        rawBlob = await cachedFetch(versionManifest.downloads["server:default"].url, (percent) => {
            progress.next(percent);
        })
    } finally {
        progress.next(undefined);
    }

    let versionKey = version.version.id + "#" + versionManifest.id;

    const paperclipJar = await openJar(versionKey, rawBlob);

    const patchedBlob = await runPaperclip(paperclipJar);
    const patchedJar = await openJar(versionKey + ".patched", patchedBlob);

    return {
        version: versionKey,
        jar: patchedJar,
        blob: patchedBlob,
        type: "paper",
        metadata: {
            sha256: versionManifest.downloads["server:default"].checksums.sha256,
        },
    };
}

async function runPaperclip(jar: Jar) {
    const downloadContextEntry = jar.entries["META-INF/download-context" as JarEntryPath];
    if (!downloadContextEntry) {
        throw new Error("Paperclip jar doesn't contain download context");
    }
    const patchesEntry = jar.entries["META-INF/patches.list" as JarEntryPath];
    if (!patchesEntry) {
        throw new Error("Paperclip jar doesn't contain patches list");
    }

    const downloadContext = (await downloadContextEntry.text()).split("\t");
    // TODO better progress
    const mcBundler = await cachedFetch(downloadContext[1]);
    const mcBundlerJar = await openJar(downloadContext[2], mcBundler);

    const patchLine = (await patchesEntry.text()).split("\n")
        .filter(line => !line.startsWith("#"))
        .map(line => line.split("\t"))
        .find(line => line[0] === "versions");

    if (!patchLine) {
        throw new Error("Paperclip jar doesn't contain a patch for versions");
    }

    const patchEntry = jar.entries["META-INF/versions/" + patchLine[5] as JarEntryPath];
    if (!patchEntry) {
        throw new Error("Paperclip jar doesn't contain a patch for versions");
    }
    const patch = await patchEntry.blob();

    const mcJarEntry = mcBundlerJar.entries["META-INF/versions/" + patchLine[4] as JarEntryPath];
    if (!mcJarEntry) {
        throw new Error("Vanilla bundler jar doesn't contain the server jar");
    }
    const mcJar = await mcJarEntry.blob();

    // todo can we show a progress bar?
    const mcJarData = new Uint8Array(await mcJar.arrayBuffer());
    const mcHash = await sha256hex(mcJarData);
    const patchFileData = new Uint8Array(await patch.arrayBuffer());
    const patchHash = await sha256hex(patchFileData);
    const patched = await bsPatch(mcJarData, patchFileData);
    const patchedHash = await sha256hex(patched);

    if (mcHash !== patchLine[1]) {
        throw new Error(`Vanilla jar hash mismatch: expected ${patchLine[1]}, got ${mcHash}`);
    } else if (patchHash !== patchLine[2]) {
        throw new Error(`Patch file hash mismatch: expected ${patchLine[2]}, got ${patchHash}`);
    } else if (patchedHash != patchLine[3]) {
        throw new Error(`Patched jar hash mismatch: expected ${patchLine[3]}, got ${patchedHash}`);
    }

    return new Blob([patched], {type: mcJar.type});
}

async function bsPatch(mcJarData: Uint8Array<ArrayBuffer>, patchFileData: Uint8Array<ArrayBuffer>) {
    console.log("Loading bspatch")
    const patcher = await loadBspatch();

    patcher.FS.writeFile("o", mcJarData);
    patcher.FS.writeFile("p", patchFileData);

    console.log("Running bspatch");
    patcher.callMain(["o", "n", "p"]);
    console.log("Done");

    return patcher.FS.readFile("n");
}

async function sha256hex(bytes: Uint8Array): Promise<string> {
    const buffer = bytes.buffer.slice(
        bytes.byteOffset,
        bytes.byteOffset + bytes.byteLength
    ) as ArrayBuffer;
    const hash = await crypto.subtle.digest("SHA-256", buffer);
    return Array.from(new Uint8Array(hash))
        .map((b) => b.toString(16).padStart(2, "0"))
        .join("");
}
