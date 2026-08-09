import {type Jar, openJar} from "../utils/Jar.ts";
import {
    BehaviorSubject,
    combineLatest,
    distinctUntilChanged,
    map,
    Observable,
    of,
    shareReplay,
    switchMap,
    tap
} from "rxjs";
import {
    type MinecraftJarMetadata,
    minecraftJarPipeline,
    minecraftVersions
} from "./MinecraftApi.ts";
import {selectedTargetVersion} from "./State.ts";
import {type PaperJarMetadata, paperJarPipeline, paperVersions} from "./PaperApi.ts";

export const CACHE_NAME = 'mcsrc-v1';
export type JarType = "minecraft" | "paper";

export interface TargetJar<T = MinecraftJarMetadata | PaperJarMetadata> {
    version: string;
    jar: Jar;
    blob: Blob;
    metadata: T;
    type: JarType
}

interface Version {
    id: string;
    jarType: JarType;
    releaseType: "release" | "snapshot";
}

const combinedVersions = combineLatest([
    minecraftVersions.pipe(
        map(versions => {
            return versions.map(v => ({
                id: v.id,
                jarType: "minecraft",
            })) as Version[];
        })
    ),
    paperVersions.pipe(
        map(versions => {
            return versions.map(v => ({
                id: v.version.id,
                jarType: "paper"
            })) as Version[];
        })
    ),
]).pipe(
    map(([minecraftVers, paperVers]) => [...minecraftVers, ...paperVers])
);

export const targetVersions = combinedVersions.pipe(
    tap(versions => {
        // On inital load, if we dont have a version selected or the selected version is not valid, default to the latest version.
        const currentVersion = selectedTargetVersion.value;
        const isValid = currentVersion !== null && versions.some(v => v.id === currentVersion);

        if (!isValid && versions.length > 0) {
            // Select the latest stable release version if it exists, otherwise fall back to the latest version
            const latestRelease = versions.find(v => v.releaseType === "release");
            const defaultVersion = latestRelease ? latestRelease.id : versions[0].id;
            selectedTargetVersion.next(defaultVersion);
        }
    }),
    shareReplay({bufferSize: 1, refCount: false})
);

export const targetVersionIds = targetVersions.pipe(
    map(versions => versions.map(v => v.id))
);

export const targetJar = targetJarPipeline(selectedTargetVersion);

export function targetJarPipeline<T>(source$: Observable<string | null>): Observable<TargetJar<T>> {
    return source$.pipe(
        distinctUntilChanged(),
        switchMap(source => {
            const pipeline = source?.startsWith("paper") ? paperJarPipeline : minecraftJarPipeline;
            return pipeline(of(source)) as Observable<TargetJar<T>>;
        }),
        shareReplay({bufferSize: 1, refCount: false})
    );
}

export async function getJson<T>(url: string): Promise<T> {
    console.log(`Fetching JSON from ${url}`);
    const response = await fetch(url);

    if (!response.ok) {
        throw new Error(`Failed to fetch JSON from ${url}: ${response.statusText}`);
    }

    return response.json();
}

export const downloadProgress = new BehaviorSubject<number | undefined>(undefined);

export async function cachedFetch(url: string, onProgress?: (percent: number) => void): Promise<Blob> {
    if (!('caches' in window)) {
        const response = await fetch(url);
        if (!response.ok) {
            throw new Error(`Failed to fetch ${url}: ${response.statusText}`);
        }
        return await consumeResponseWithProgress(response, onProgress);
    }

    const cache = await caches.open(CACHE_NAME);
    const cachedResponse = await cache.match(url);
    if (cachedResponse) {
        return await cachedResponse.blob();
    }

    const response = await fetch(url);
    if (!response.ok) {
        throw new Error(`Failed to fetch ${url}: ${response.statusText}`);
    }

    const blob = await consumeResponseWithProgress(response, onProgress);

    // Cache the blob after it's been consumed
    await cache.put(url, new Response(blob, {
        headers: response.headers
    }));

    return blob;
}

async function consumeResponseWithProgress(response: Response, onProgress?: (percent: number) => void): Promise<Blob> {
    const contentLength = response.headers.get('content-length');
    const total = contentLength ? parseInt(contentLength, 10) : 0;

    if (!response.body || total === 0 || !onProgress) {
        return await response.blob();
    }

    const reader = response.body.getReader();
    const chunks: Uint8Array<ArrayBuffer>[] = [];
    let receivedLength = 0;
    let lastPercent = -1;

    while (true) {
        const {done, value} = await reader.read();
        if (done) break;

        chunks.push(value);
        receivedLength += value.length;

        const percent = Math.round((receivedLength / total) * 100);

        if (percent !== lastPercent) {
            onProgress(percent);
            lastPercent = percent;
        }
    }

    return new Blob(chunks);
}
