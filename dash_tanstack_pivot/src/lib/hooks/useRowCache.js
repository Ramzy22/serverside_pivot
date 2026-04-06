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
    const pinnedWindowRef = useRef({
        epoch: 0,
        startBlock: null,
        endBlock: null,
        buffer: 0
    });
    
    // Version to force re-renders when cache updates
    const [cacheVersion, setCacheVersion] = useState(0);

    // Current request version to handle race conditions
    const requestVersionRef = useRef(0);

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
        pinnedWindowRef.current = {
            epoch: normalizedEpoch,
            startBlock: null,
            endBlock: null,
            buffer: 0
        };

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

    const setPinnedRange = useCallback((minBlock, maxBlock, buffer = 0, epoch = currentEpochRef.current) => {
        if (!Number.isFinite(minBlock) || !Number.isFinite(maxBlock)) return;
        pinnedWindowRef.current = {
            epoch,
            startBlock: Math.min(minBlock, maxBlock),
            endBlock: Math.max(minBlock, maxBlock),
            buffer: Math.max(0, buffer)
        };
    }, []);

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
        const pinnedStart = Math.max(0, pinned.startBlock - pinned.buffer);
        const pinnedEnd = pinned.endBlock + pinned.buffer;
        if (blockIndex < pinnedStart) return pinnedStart - blockIndex;
        if (blockIndex > pinnedEnd) return blockIndex - pinnedEnd;
        return 0;
    }, []);

    const evictOverflow = useCallback((epoch = currentEpochRef.current) => {
        let changed = false;
        while (cacheRef.current.size > maxBlocks) {
            let evictionKey = null;
            let evictionScore = null;

            for (const [key, block] of cacheRef.current.entries()) {
                const { epoch: keyEpoch, blockIndex: keyBlockIndex } = parseKey(String(key));
                const distance = blockDistanceFromPinned(keyBlockIndex, keyEpoch);
                const isPinned = distance === 0;
                const age = Number(block?.timestamp) || 0;
                const score = [
                    isPinned ? 1 : 0,
                    distance,
                    -age,
                ];
                if (
                    evictionScore === null ||
                    score[0] < evictionScore[0] ||
                    (score[0] === evictionScore[0] && score[1] > evictionScore[1]) ||
                    (score[0] === evictionScore[0] && score[1] === evictionScore[1] && score[2] > evictionScore[2])
                ) {
                    evictionScore = score;
                    evictionKey = key;
                }
            }

            if (evictionKey === null) break;
            cacheRef.current.delete(evictionKey);
            changed = true;
        }
        return changed;
    }, [blockDistanceFromPinned, maxBlocks, parseKey]);

    /**
     * Mark a block as loading
     * Returns the version of this request
     */
    const setBlockLoading = useCallback((blockIndex, requestVersion = null, epoch = currentEpochRef.current) => {
        const version = (requestVersion !== null && requestVersion !== undefined) ? requestVersion : (requestVersionRef.current + 1);
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
        
        evictOverflow(epoch);

        // Force an immediate repaint so loading placeholders appear as soon as
        // a request is queued (not only when the response arrives).
        setCacheVersion(v => v + 1);
        
        return version;
    }, [evictOverflow, makeKey]);

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

        const previousRows = block && Array.isArray(block.rows) ? block.rows : null;
        const mergedRows = Array.isArray(rows) && previousRows && previousRows.length > 0
            ? (() => {
                const previousByKey = new Map();
                previousRows.forEach((row, index) => {
                    previousByKey.set(getRowMergeKey(row, index), row);
                });
                return rows.map((row, index) => {
                    if (!row || typeof row !== 'object') return row;
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
        setCacheVersion(v => v + 1);
    }, [evictOverflow, getRowMergeKey, makeKey]);

    /**
     * Clear the entire cache (e.g. on filter/sort change)
     */
    const clearCache = useCallback(() => {
        cacheRef.current.clear();
        pinnedWindowRef.current = {
            epoch: currentEpochRef.current,
            startBlock: null,
            endBlock: null,
            buffer: 0
        };
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
     * Soft-invalidate a specific contiguous block range while preserving rows.
     */
    const softInvalidateRange = useCallback((startBlock, endBlock, epoch = currentEpochRef.current) => {
        if (!Number.isFinite(startBlock) || !Number.isFinite(endBlock)) return;
        const normalizedStart = Math.min(startBlock, endBlock);
        const normalizedEnd = Math.max(startBlock, endBlock);
        let changed = false;
        for (const [key, block] of Array.from(cacheRef.current.entries())) {
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
            const isPinned = blockDistanceFromPinned(keyBlockIndex, keyEpoch) === 0;
            if (!isPinned && (keyBlockIndex < keepFrom || keyBlockIndex > keepTo)) {
                cacheRef.current.delete(key);
                changed = true;
            }
        }

        const overflowChanged = evictOverflow(epoch);

        if (changed || overflowChanged) {
            setCacheVersion(v => v + 1);
        }
    }, [blockDistanceFromPinned, evictOverflow, parseKey]);

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

    const getLoadedBlocks = useCallback((epoch = currentEpochRef.current) => {
        const blocks = [];
        for (const [key, block] of cacheRef.current.entries()) {
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
