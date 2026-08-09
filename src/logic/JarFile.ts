import { BehaviorSubject, asyncScheduler, combineLatest, distinct, distinctUntilChanged, map, Observable, switchMap, throttleTime } from 'rxjs';
import { targetJar } from './JarProvider';
import { performSearch } from './Search';
import { searchQuery } from './State';
import { isClassFilePath, type ClassFilePath } from '../utils/Names';

export const fileList = targetJar.pipe(
    distinctUntilChanged(),
    map(jar => Object.keys(jar.jar.entries))
);

// File list that only contains outer class files
export const classesList = fileList.pipe(
    map(files => files.filter((file): file is ClassFilePath => isClassFilePath(file) && !file.includes('$')))
);

const debouncedSearchQuery: Observable<string> = searchQuery.pipe(
    throttleTime(200, asyncScheduler, { trailing: true }),
    distinctUntilChanged()
);

export const searchResults: Observable<ClassFilePath[]> = combineLatest([classesList, debouncedSearchQuery]).pipe(
    switchMap(([classes, query]) => {
        return [performSearch(query, classes)];
    })
);

export const isSearching = searchQuery.pipe(
    map((query) => query.length > 0)
);
