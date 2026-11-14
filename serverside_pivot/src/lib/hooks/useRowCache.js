import { useRef, useCallback, useState } from 'react';

/**
 * Manages client-side caching of server-side rows using a block-based strategy.
 * 
 * @param {Object} [options]
 * @param {number} [options.blockSize] - Number of rows per block (default: 100)
 * @param {number} [options.maxBlocks] - Maximum number of cached blocks (default: 500)
 */
export const useRowCache = ({ blockSize = 100, maxBlocks = 500 } = {}) => {
    // Cache Structure: Map<`${epoch}:${blockIndex}`, { status, rows, timestamp, version, epoch, blockIndex }>
    // We use a Map which preserves insertion order, so keys() is an iterator from oldest to newest.
    /** @type {any} */
    const cacheRef = useRef(new Map());
    /** @type {any} */
    const currentEpochRef = useRef(0);
    /** @type {any} */
    const pinnedWindowRef = useRef({
        epoch: 0,
        startBlock: null,
        endBlock: null,
        buffer: 0
    });
    
    // Version to force re-renders when cache updates
    /** @type {[any, (...args: any[]) => any]} */
    const [cacheVersion, setCacheVersion] = useState(0);

    // Current request version to handle race conditions
    /** @type {any} */
    const requestVersionRef = useRef(0);

    /** @type {(...args: any[]) => any} */
    const getRowMergeKey = useCallback((row, fallbackIndex) => {
        if (!row || typeof row !== 'object') return `__index__:${fallbackIndex}`;
        if (typeof row._path === 'string' && row._path) return `path:${row._path}`;
        if (typeof row.id === 'string' && row.id) return `id:${row.id}`;
        if (typeof row._id === 'string' && row._id) return `gid:${row._id}`;
        if (typeof row.uuid === 'string' && row.uuid) return `uuid:${row.uuid}`;
        if (Number.isFinite(Number(row.__virtualIndex))) return `virtual:${Number(row.__virtualIndex)}`;
        return `__index__:${fallbackIndex}`;
    }, []);

    /**
     * Get a block from the cache
      @type {(...args: any[]) => any} */
    const makeKey = useCallback((epoch, blockIndex) => `${epoch}:${blockIndex}`, []);

    /** @type {(...args: any[]) => any} */
    const parseKey = useCallback((key) => {
        /** @type {any} */
        const sepIndex = key.indexOf(':');
        if (sepIndex < 0) return { epoch: 0, blockIndex: Number(key) || 0 };
        /** @type {any} */
        const epoch = Number(key.slice(0, sepIndex));
        /** @type {any} */
        const blockIndex = Number(key.slice(sepIndex + 1));
        return {
            epoch: Number.isFinite(epoch) ? epoch : 0,
            blockIndex: Number.isFinite(blockIndex) ? blockIndex : 0
        };
    }, []);

    /** @type {(...args: any[]) => any} */
    const setCurrentEpoch = useCallback((epoch) => {
        /** @type {any} */
        const normalizedEpoch = Number(epoch);
        if (!Number.isFinite(normalizedEpoch)) return;
        if (normalizedEpoch === currentEpochRef.current) return;

        currentEpochRef.current = normalizedEpoch;
        pinnedWindowRef.current = {
            epoch: normalizedEpoch,
            startBlock: null,
            endBlock: null,
            buffer: 0
        };

        // Strict GC: old epochs are never reused, so purge immediately.
        let changed = false;
        for (const key of Array.from(cacheRef.current.keys())) {
            /** @type {any} */
            const { epoch: keyEpoch } = parseKey(String(key));
            if (keyEpoch < normalizedEpoch) {
                cacheRef.current.delete(key);
                changed = true;
            }
        }
        if (changed) setCacheVersion(/** @param {any} [v] @returns {any} */ v => v + 1);
    }, [parseKey]);

    /** @type {(...args: any[]) => any} */
    const getBlock = useCallback((blockIndex, epoch = currentEpochRef.current) => {
        // Simple read - no LRU re-ordering on read to prevent excessive state updates/map operations during render loops
        return cacheRef.current.get(makeKey(epoch, blockIndex));
    }, [makeKey]);

    /** @type {(...args: any[]) => any} */
    const setPinnedRange = useCallback((minBlock, maxBlock, buffer = 0, epoch = currentEpochRef.current) => {
        if (!Number.isFinite(minBlock) || !Number.isFinite(maxBlock)) return;
        pinnedWindowRef.current = {
            epoch,
            startBlock: Math.min(minBlock, maxBlock),
            endBlock: Math.max(minBlock, maxBlock),
            buffer: Math.max(0, buffer)
        };
    }, []);

    /** @type {(...args: any[]) => any} */
    const blockDistanceFromPinned = useCallback((blockIndex, epoch = currentEpochRef.current) => {
        const pinned = pinnedWindowRef.current;
        if (
            !pinned ||
            pinned.epoch !== epoch ||
            pinned.startBlock === null ||
            pinned.endBlock === null
        ) {
            return Number.MAX_SAFE_INTEGER;
        }
        /** @type {any} */
        const pinnedStart = Math.max(0, pinned.startBlock - pinned.buffer);
        /** @type {any} */
        const pinnedEnd = pinned.endBlock + pinned.buffer;
        if (blockIndex < pinnedStart) return pinnedStart - blockIndex;
        if (blockIndex > pinnedEnd) return blockIndex - pinnedEnd;
        return 0;
    }, []);

    /** @type {(...args: any[]) => any} */
    const evictOverflow = useCallback((epoch = currentEpochRef.current) => {
        let changed = false;
        while (cacheRef.current.size > maxBlocks) {
            // The Map preserves insertion order and every setBlockLoading/setBlockLoaded
            // moves its entry to the end via delete+set, so the front is always the
            // oldest (least-recently-used) entry. Scan from the front and evict the
            // first non-pinned block — O(k) where k = pinned blocks near the front,
            // typically 0, never more than a handful.
            /** @type {any} */
            let evictionKey = null;
            for (const [key, _block] of cacheRef.current.entries()) {
                /** @type {any} */
                const { epoch: keyEpoch, blockIndex: keyBlockIndex } = parseKey(String(key));
                if (blockDistanceFromPinned(keyBlockIndex, keyEpoch) !== 0) {
                    evictionKey = key;
                    break;
                }
            }
            if (evictionKey === null) break; // All remaining blocks are pinned
            cacheRef.current.delete(evictionKey);
            changed = true;
        }
        return changed;
    }, [blockDistanceFromPinned, maxBlocks, parseKey]);

    /**
     * Mark a block as loading
     * Returns the version of this request
      @type {(...args: any[]) => any} */
    const setBlockLoading = useCallback((blockIndex, requestVersion = null, epoch = currentEpochRef.current) => {
        /** @type {any} */
        const version = (requestVersion !== null && requestVersion !== undefined) ? requestVersion : (requestVersionRef.current + 1);
        requestVersionRef.current = version;
        /** @type {any} */
        const cacheKey = makeKey(epoch, blockIndex);
        
        /** @type {any} */
        let existingRows = null;

        // Remove if exists (move to end)
        if (cacheRef.current.has(cacheKey)) {
            /** @type {any} */
            const existingBlock = cacheRef.current.get(cacheKey);
            if (existingBlock && existingBlock.rows) {
                existingRows = existingBlock.rows;
            }
            cacheRef.current.delete(cacheKey);
        }
        
        // Preserve existing rows for smooth transition (Stale-While-Revalidate)
        cacheRef.current.set(cacheKey, {
            status: 'loading',
            rows: existingRows,
            timestamp: Date.now(),
            version,
            epoch,
            blockIndex
        });
        
        evictOverflow(epoch);

        // Force an immediate repaint so loading placeholders appear as soon as
        // a request is queued (not only when the response arrives).
        setCacheVersion(/** @param {any} [v] @returns {any} */ v => v + 1);
        
        return version;
    }, [evictOverflow, makeKey]);

    /**
     * Store loaded rows into a block
      @type {(...args: any[]) => any} */
    const setBlockLoaded = useCallback((blockIndex, rows, requestVersion, isComplete = true, epoch = currentEpochRef.current, colStart = null, colEnd = null) => {
        /** @type {any} */
        const cacheKey = makeKey(epoch, blockIndex);
        /** @type {any} */
        const block = cacheRef.current.get(cacheKey);

        // Stale check: never allow an older response to overwrite a newer block.
        if (block && block.version > requestVersion) {
            return;
        }

        /** @type {any} */
        const previousRows = block && Array.isArray(block.rows) ? block.rows : null;
        /** @type {any} */
        const mergedRows = Array.isArray(rows) && previousRows && previousRows.length > 0
            ? (() => {
                /** @type {any} */
                const previousByKey = new Map();
                previousRows.forEach(/** @param {any} [row] @param {any} [index] @returns {any} */ (row, index) => {
                    previousByKey.set(getRowMergeKey(row, index), row);
                });
                return rows.map((row, index) => {
                    if (!row || typeof row !== 'object') return row;
                    /** @type {any} */
                    const previousRow = previousByKey.get(getRowMergeKey(row, index));
                    return previousRow && typeof previousRow === 'object'
                        ? { ...previousRow, ...row }
                        : row;
                });
            })()
            : rows;

        // Move to end (most recently used/updated)
        cacheRef.current.delete(cacheKey);
        cacheRef.current.set(cacheKey, {
            status: isComplete ? 'loaded' : 'partial',
            rows: mergedRows,
            timestamp: Date.now(),
            version: requestVersion || (block ? block.version : 0),
            epoch,
            blockIndex,
            colStart,
            colEnd
        });
        
        evictOverflow(epoch);
        
        // Trigger update
        setCacheVersion(/** @param {any} [v] @returns {any} */ v => v + 1);
    }, [evictOverflow, getRowMergeKey, makeKey]);

    /**
     * Clear the entire cache (e.g. on filter/sort change)
      @type {(...args: any[]) => any} */
    const clearCache = useCallback(() => {
        cacheRef.current.clear();
        pinnedWindowRef.current = {
            epoch: currentEpochRef.current,
            startBlock: null,
            endBlock: null,
            buffer: 0
        };
        setCacheVersion(/** @param {any} [v] @returns {any} */ v => v + 1);
    }, []);

    /**
     * Hard-delete all blocks at or after blockIndex.
     * Used after the expansion response has already updated the anchor block so
     * only the subsequent blocks (whose row indices shifted) need re-fetching.
      @type {(...args: any[]) => any} */
    const invalidateFromBlock = useCallback((blockIndex, epoch = currentEpochRef.current) => {
        let changed = false;
        for (const key of Array.from(cacheRef.current.keys())) {
            /** @type {any} */
            const { epoch: keyEpoch, blockIndex: keyBlockIndex } = parseKey(String(key));
            if (keyEpoch === epoch && keyBlockIndex >= blockIndex) {
                cacheRef.current.delete(key);
                changed = true;
            }
        }
        if (changed) setCacheVersion(/** @param {any} [v] @returns {any} */ v => v + 1);
    }, [parseKey]);

    /**
     * Soft-invalidate all blocks at or after blockIndex: mark them as 'partial'
     * while keeping their existing rows (stale-while-revalidate).  The viewport
     * effect's `isPartial` check will immediately queue a background refresh for
     * visible blocks, so users never see a skeleton flash — they see slightly stale
     * data for the duration of one network round-trip, then the correct rows appear.
      @type {(...args: any[]) => any} */
    const softInvalidateFromBlock = useCallback((blockIndex, epoch = currentEpochRef.current) => {
        let changed = false;
        for (const [key, block] of Array.from(cacheRef.current.entries())) {
            /** @type {any} */
            const { epoch: keyEpoch, blockIndex: keyBlockIndex } = parseKey(String(key));
            if (keyEpoch === epoch && keyBlockIndex >= blockIndex && block.status !== 'partial') {
                cacheRef.current.set(key, { ...block, status: 'partial' });
                changed = true;
            }
        }
        if (changed) setCacheVersion(/** @param {any} [v] @returns {any} */ v => v + 1);
    }, [parseKey]);

    /**
     * Soft-invalidate a specific contiguous block range while preserving rows.
      @type {(...args: any[]) => any} */
    const softInvalidateRange = useCallback((startBlock, endBlock, epoch = currentEpochRef.current) => {
        if (!Number.isFinite(startBlock) || !Number.isFinite(endBlock)) return;
        /** @type {any} */
        const normalizedStart = Math.min(startBlock, endBlock);
        /** @type {any} */
        const normalizedEnd = Math.max(startBlock, endBlock);
        let changed = false;
        for (const [key, block] of Array.from(cacheRef.current.entries())) {
            /** @type {any} */
            const { epoch: keyEpoch, blockIndex: keyBlockIndex } = parseKey(String(key));
            if (
                keyEpoch === epoch
                && keyBlockIndex >= normalizedStart
                && keyBlockIndex <= normalizedEnd
                && block.status !== 'partial'
            ) {
                cacheRef.current.set(key, { ...block, status: 'partial' });
                changed = true;
            }
        }
        if (changed) setCacheVersion(/** @param {any} [v] @returns {any} */ v => v + 1);
    }, [parseKey]);

    /**
     * Keep only blocks within a sliding window around the viewport.
      @type {(...args: any[]) => any} */
    const pruneToRange = useCallback((minBlock, maxBlock, buffer = 1, epoch = currentEpochRef.current) => {
        /** @type {any} */
        const keepFrom = Math.max(0, minBlock - buffer);
        /** @type {any} */
        const keepTo = maxBlock + buffer;
        let changed = false;

        for (const key of Array.from(cacheRef.current.keys())) {
            /** @type {any} */
            const { epoch: keyEpoch, blockIndex: keyBlockIndex } = parseKey(String(key));
            if (keyEpoch !== epoch) continue;
            /** @type {any} */
            const isPinned = blockDistanceFromPinned(keyBlockIndex, keyEpoch) === 0;
            if (!isPinned && (keyBlockIndex < keepFrom || keyBlockIndex > keepTo)) {
                cacheRef.current.delete(key);
                changed = true;
            }
        }

        /** @type {any} */
        const overflowChanged = evictOverflow(epoch);

        if (changed || overflowChanged) {
            setCacheVersion(/** @param {any} [v] @returns {any} */ v => v + 1);
        }
    }, [blockDistanceFromPinned, evictOverflow, parseKey]);

    /**
     * Get a specific row from the cache
      @type {(...args: any[]) => any} */
    const getRow = useCallback((rowIndex, epoch = currentEpochRef.current) => {
        /** @type {any} */
        const blockIndex = Math.floor(rowIndex / blockSize);
        /** @type {any} */
        const block = cacheRef.current.get(makeKey(epoch, blockIndex));
        
        // Allow returning rows even if status is 'loading' (Stale-While-Revalidate)
        if (!block || !block.rows) {
            return null;
        }

        /** @type {any} */
        const internalIndex = rowIndex % blockSize;
        return block.rows[internalIndex];
    }, [blockSize, makeKey]);

    /** @type {(...args: any[]) => any} */
    const getLoadedBlocks = useCallback((epoch = currentEpochRef.current) => {
        /** @type {any[]} */
        const blocks = [];
        for (const [key, block] of cacheRef.current.entries()) {
            /** @type {any} */
            const { epoch: keyEpoch } = parseKey(String(key));
            if (keyEpoch !== epoch) continue;
            if (!block || !Array.isArray(block.rows) || block.rows.length === 0) continue;
            blocks.push(block);
        }
        blocks.sort((left, right) => left.blockIndex - right.blockIndex);
        return blocks;
    }, [parseKey]);

    return {
        setCurrentEpoch,
        getBlock,
        setBlockLoading,
        setBlockLoaded,
        clearCache,
        invalidateFromBlock,
        softInvalidateFromBlock,
        softInvalidateRange,
        pruneToRange,
        setPinnedRange,
        getRow,
        getLoadedBlocks,
        cacheVersion,
        blockSize,
        currentEpochRef
    };
};
