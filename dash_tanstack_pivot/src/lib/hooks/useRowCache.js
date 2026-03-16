import { useRef, useCallback, useState } from 'react';

/**
 * Manages client-side caching of server-side rows using a block-based strategy.
 * 
 * @param {Object} options
 * @param {number} options.blockSize - Number of rows per block (default: 100)
 */
export const useRowCache = ({ blockSize = 100, maxBlocks = 500 } = {}) => {
    // Cache Structure: Map<`${epoch}:${blockIndex}`, { status, rows, timestamp, version, epoch, blockIndex }>
    // We use a Map which preserves insertion order, so keys() is an iterator from oldest to newest.
    const cacheRef = useRef(new Map());
    const currentEpochRef = useRef(0);
    
    // Version to force re-renders when cache updates
    const [cacheVersion, setCacheVersion] = useState(0);

    // Current request version to handle race conditions
    const requestVersionRef = useRef(0);

    /**
     * Get a block from the cache
     */
    const makeKey = useCallback((epoch, blockIndex) => `${epoch}:${blockIndex}`, []);

    const parseKey = useCallback((key) => {
        const sepIndex = key.indexOf(':');
        if (sepIndex < 0) return { epoch: 0, blockIndex: Number(key) || 0 };
        const epoch = Number(key.slice(0, sepIndex));
        const blockIndex = Number(key.slice(sepIndex + 1));
        return {
            epoch: Number.isFinite(epoch) ? epoch : 0,
            blockIndex: Number.isFinite(blockIndex) ? blockIndex : 0
        };
    }, []);

    const setCurrentEpoch = useCallback((epoch) => {
        const normalizedEpoch = Number(epoch);
        if (!Number.isFinite(normalizedEpoch)) return;
        if (normalizedEpoch === currentEpochRef.current) return;

        currentEpochRef.current = normalizedEpoch;

        // Strict GC: old epochs are never reused, so purge immediately.
        let changed = false;
        for (const key of Array.from(cacheRef.current.keys())) {
            const { epoch: keyEpoch } = parseKey(String(key));
            if (keyEpoch < normalizedEpoch) {
                cacheRef.current.delete(key);
                changed = true;
            }
        }
        if (changed) setCacheVersion(v => v + 1);
    }, [parseKey]);

    const getBlock = useCallback((blockIndex, epoch = currentEpochRef.current) => {
        // Simple read - no LRU re-ordering on read to prevent excessive state updates/map operations during render loops
        return cacheRef.current.get(makeKey(epoch, blockIndex));
    }, [makeKey]);

    /**
     * Mark a block as loading
     * Returns the version of this request
     */
    const setBlockLoading = useCallback((blockIndex, requestVersion = null, epoch = currentEpochRef.current) => {
        const version = requestVersion ?? (requestVersionRef.current + 1);
        requestVersionRef.current = version;
        const cacheKey = makeKey(epoch, blockIndex);
        
        let existingRows = null;

        // Remove if exists (move to end)
        if (cacheRef.current.has(cacheKey)) {
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
        
        // Evict if over limit
        if (cacheRef.current.size > maxBlocks) {
            const firstKey = cacheRef.current.keys().next().value;
            cacheRef.current.delete(firstKey);
        }

        // Force an immediate repaint so loading placeholders appear as soon as
        // a request is queued (not only when the response arrives).
        setCacheVersion(v => v + 1);
        
        return version;
    }, [maxBlocks, makeKey]);

    /**
     * Store loaded rows into a block
     */
    const setBlockLoaded = useCallback((blockIndex, rows, requestVersion, isComplete = true, epoch = currentEpochRef.current, colStart = null, colEnd = null) => {
        const cacheKey = makeKey(epoch, blockIndex);
        const block = cacheRef.current.get(cacheKey);

        // Stale check: never allow an older response to overwrite a newer block.
        if (block && block.version > requestVersion) {
            return;
        }

        // Move to end (most recently used/updated)
        cacheRef.current.delete(cacheKey);
        cacheRef.current.set(cacheKey, {
            status: isComplete ? 'loaded' : 'partial',
            rows,
            timestamp: Date.now(),
            version: requestVersion || (block ? block.version : 0),
            epoch,
            blockIndex,
            colStart,
            colEnd
        });
        
        // Evict if over limit (safety check)
        while (cacheRef.current.size > maxBlocks) {
            const firstKey = cacheRef.current.keys().next().value;
            cacheRef.current.delete(firstKey);
        }
        
        // Trigger update
        setCacheVersion(v => v + 1);
    }, [maxBlocks, makeKey]);

    /**
     * Clear the entire cache (e.g. on filter/sort change)
     */
    const clearCache = useCallback(() => {
        cacheRef.current.clear();
        setCacheVersion(v => v + 1);
    }, []);

    /**
     * Hard-delete all blocks at or after blockIndex.
     * Used after the expansion response has already updated the anchor block so
     * only the subsequent blocks (whose row indices shifted) need re-fetching.
     */
    const invalidateFromBlock = useCallback((blockIndex, epoch = currentEpochRef.current) => {
        let changed = false;
        for (const key of Array.from(cacheRef.current.keys())) {
            const { epoch: keyEpoch, blockIndex: keyBlockIndex } = parseKey(String(key));
            if (keyEpoch === epoch && keyBlockIndex >= blockIndex) {
                cacheRef.current.delete(key);
                changed = true;
            }
        }
        if (changed) setCacheVersion(v => v + 1);
    }, [parseKey]);

    /**
     * Soft-invalidate all blocks at or after blockIndex: mark them as 'partial'
     * while keeping their existing rows (stale-while-revalidate).  The viewport
     * effect's `isPartial` check will immediately queue a background refresh for
     * visible blocks, so users never see a skeleton flash — they see slightly stale
     * data for the duration of one network round-trip, then the correct rows appear.
     */
    const softInvalidateFromBlock = useCallback((blockIndex, epoch = currentEpochRef.current) => {
        let changed = false;
        for (const [key, block] of Array.from(cacheRef.current.entries())) {
            const { epoch: keyEpoch, blockIndex: keyBlockIndex } = parseKey(String(key));
            if (keyEpoch === epoch && keyBlockIndex >= blockIndex && block.status !== 'partial') {
                cacheRef.current.set(key, { ...block, status: 'partial' });
                changed = true;
            }
        }
        if (changed) setCacheVersion(v => v + 1);
    }, [parseKey]);

    /**
     * Keep only blocks within a sliding window around the viewport.
     */
    const pruneToRange = useCallback((minBlock, maxBlock, buffer = 1, epoch = currentEpochRef.current) => {
        const keepFrom = Math.max(0, minBlock - buffer);
        const keepTo = maxBlock + buffer;
        let changed = false;

        for (const key of Array.from(cacheRef.current.keys())) {
            const { epoch: keyEpoch, blockIndex: keyBlockIndex } = parseKey(String(key));
            if (keyEpoch !== epoch) continue;
            if (keyBlockIndex < keepFrom || keyBlockIndex > keepTo) {
                cacheRef.current.delete(key);
                changed = true;
            }
        }

        if (changed) {
            setCacheVersion(v => v + 1);
        }
    }, [parseKey]);

    /**
     * Get a specific row from the cache
     */
    const getRow = useCallback((rowIndex, epoch = currentEpochRef.current) => {
        const blockIndex = Math.floor(rowIndex / blockSize);
        const block = cacheRef.current.get(makeKey(epoch, blockIndex));
        
        // Allow returning rows even if status is 'loading' (Stale-While-Revalidate)
        if (!block || !block.rows) {
            return null;
        }

        const internalIndex = rowIndex % blockSize;
        return block.rows[internalIndex];
    }, [blockSize, makeKey]);

    return {
        setCurrentEpoch,
        getBlock,
        setBlockLoading,
        setBlockLoaded,
        clearCache,
        invalidateFromBlock,
        softInvalidateFromBlock,
        pruneToRange,
        getRow,
        cacheVersion,
        blockSize,
        currentEpochRef
    };
};
