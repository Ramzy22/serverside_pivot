const ABSOLUTE_SORT_TYPES = new Set(['absolute', 'abs', 'absolute_value', 'absolute-value']);
const SORT_METADATA_KEYS = [
    'absoluteSort',
    'sortType',
    'sortMode',
    'sortKeyField',
    'semanticType',
    'sortSemantic',
    'nulls',
];

export const isAbsoluteSortType = (value) => (
    ABSOLUTE_SORT_TYPES.has(String(value || '').trim().toLowerCase())
);

export const isAbsoluteSortSpecifier = (sortSpec) => {
    if (!sortSpec || typeof sortSpec !== 'object') return false;
    return sortSpec.absoluteSort === true || isAbsoluteSortType(sortSpec.sortType || sortSpec.sortMode);
};

export const findSortSpecifier = (sorting, columnId) => {
    if (!Array.isArray(sorting) || !columnId) return null;
    return sorting.find((sortSpec) => sortSpec && sortSpec.id === columnId) || null;
};

export const mergeSortSpecifierMetadata = (sortSpec, previousSortSpec = null) => {
    if (!sortSpec || typeof sortSpec !== 'object') return sortSpec;
    if (!previousSortSpec || typeof previousSortSpec !== 'object') return sortSpec;
    const merged = { ...sortSpec };
    SORT_METADATA_KEYS.forEach((key) => {
        if (merged[key] === undefined && previousSortSpec[key] !== undefined) {
            merged[key] = previousSortSpec[key];
        }
    });
    return merged;
};

export const normalizeSortingState = (sorting, previousSorting = []) => {
    if (!Array.isArray(sorting)) return [];
    const previousById = new Map();
    if (Array.isArray(previousSorting)) {
        previousSorting.forEach((sortSpec) => {
            if (sortSpec && sortSpec.id && !previousById.has(sortSpec.id)) {
                previousById.set(sortSpec.id, sortSpec);
            }
        });
    }

    const seenIds = new Set();
    return sorting
        .map((sortSpec) => {
            if (!sortSpec || !sortSpec.id || seenIds.has(sortSpec.id)) return null;
            seenIds.add(sortSpec.id);
            return mergeSortSpecifierMetadata(
                {
                    ...sortSpec,
                    id: sortSpec.id,
                    desc: Boolean(sortSpec.desc),
                },
                previousById.get(sortSpec.id)
            );
        })
        .filter(Boolean);
};

export const updateSortingForColumn = ({
    sorting,
    columnId,
    desc,
    sortMetadata = {},
    append = false,
}) => {
    if (!columnId || columnId === '__row_number__') return Array.isArray(sorting) ? sorting : [];
    const nextSortSpec = {
        id: columnId,
        desc: Boolean(desc),
        ...(sortMetadata || {}),
    };
    if (!append) return [nextSortSpec];

    const baseSorting = Array.isArray(sorting) ? sorting : [];
    const nextSorting = [];
    const seenIds = new Set();
    let updatedExisting = false;
    baseSorting.forEach((sortSpec) => {
        if (!sortSpec || !sortSpec.id || seenIds.has(sortSpec.id)) return;
        seenIds.add(sortSpec.id);
        if (sortSpec.id === columnId) {
            nextSorting.push(nextSortSpec);
            updatedExisting = true;
            return;
        }
        nextSorting.push(sortSpec);
    });
    if (!updatedExisting) {
        nextSorting.push(nextSortSpec);
    }
    return nextSorting;
};

export const resolveColumnSortOptions = (sortOptions, columnId) => {
    if (!sortOptions || typeof sortOptions !== 'object' || !columnId) return {};
    const columnOptions = sortOptions.columnOptions && typeof sortOptions.columnOptions === 'object'
        ? sortOptions.columnOptions
        : {};
    const options = columnOptions[columnId];
    return options && typeof options === 'object' ? options : {};
};

export const shouldUseAbsoluteSort = (sorting, sortOptions, columnId) => (
    isAbsoluteSortSpecifier(findSortSpecifier(sorting, columnId))
    || isAbsoluteSortSpecifier(resolveColumnSortOptions(sortOptions, columnId))
);
