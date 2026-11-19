/**
 * Pure row-motion geometry & timing helpers shared by PivotTableBody.
 * Kept side-effect-free so expand/collapse synchronization invariants can be
 * verified directly in unit tests.
 */

export const ROW_MOTION_EASING = 'cubic-bezier(0.22, 1, 0.36, 1)';
export const MAX_FLIP_DISTANCE_PX = 1400;
export const MAX_SHEET_TRAVEL_PX = 480;
export const SKELETON_FILL_DURATION_MS = 220;
export const SKELETON_FILL_STAGGER_MS = 14;
export const SKELETON_FILL_MAX_STAGGER_MS = 110;
export const SKELETON_FILL_OFFSET_Y_PX = -6;

/**
 * Compute a cohesive, distance-scaled motion duration (ms) for row expand/collapse glides.
 * Short 1-3 row shifts feel crisp (~215-240ms); larger multi-row unfolds breathe smoothly up to 300ms.
 * @param {number} distancePx
 * @returns {number}
 */
export function computeSlideDurationMs(distancePx) {
    const d = Math.abs(Number(distancePx) || 0);
    if (d <= 0) return 215;
    return Math.round(Math.max(210, Math.min(300, 185 + (Math.sqrt(d) * 4.5))));
}

/**
 * Compute deterministic, transform-free layout top for the bottom-pinned row group
 * relative to the scroll container's top edge (immune to pending WAAPI transforms & forced reflow).
 * @param {{
 *   stickyHeaderHeight: number,
 *   topPinnedCount: number,
 *   bottomPinnedCount: number,
 *   containerBodyHeight: number,
 *   rowHeight: number,
 *   scrollTop: number,
 *   clientHeight: number,
 * }} params
 * @returns {number}
 */
export function computeBottomPinnedLayoutTop({
    stickyHeaderHeight,
    topPinnedCount,
    bottomPinnedCount,
    containerBodyHeight,
    rowHeight,
    scrollTop,
    clientHeight,
}) {
    const safeRowHeight = Number(rowHeight) > 0 ? Number(rowHeight) : 36;
    const bottomGroupHeight = Math.max(0, Number(bottomPinnedCount) || 0) * safeRowHeight;
    const unclampedRelTop = (
        (Number(stickyHeaderHeight) || 0)
        + (Math.max(0, Number(topPinnedCount) || 0) * safeRowHeight)
        + Math.max(0, Number(containerBodyHeight) || 0)
        - (Number(scrollTop) || 0)
    );
    const safeClientHeight = Number(clientHeight) || 0;
    const stickyFloorRelTop = safeClientHeight > 0
        ? Math.max(0, safeClientHeight - bottomGroupHeight)
        : unclampedRelTop;
    return Math.min(unclampedRelTop, stickyFloorRelTop);
}

/**
 * Build a rigid-sheet unfold plan for newly revealed child rows on expand.
 * Each newly revealed child is grouped under its deepest pre-existing visible ancestor
 * (present in prevTopByKey). Every child in a group glides by the exact same sheet travel
 * distance as the siblings descending below it, maintaining constant row-height spacing
 * and staying occluded behind the parent row (zIndex: 2 vs parent zIndex: 4) until clearing
 * the parent's bottom border.
 *
 * @param {{
 *   committedSnapshots: Array<any>,
 *   prevTopByKey: Map<string, number>,
 *   hasRecentlyExpandedAncestor: (rowPath: string) => boolean,
 *   rowHeight: number,
 * }} params
 * @returns {{
 *   revealByKey: Map<string, { fromTop: number, toTop: number, travelPx: number, parentKey: string }>,
 *   maxRevealTravelPx: number,
 * }}
 */
export function buildExpandRevealPlan({
    committedSnapshots,
    prevTopByKey,
    hasRecentlyExpandedAncestor,
    rowHeight,
}) {
    /** @type {Map<string, { fromTop: number, toTop: number, travelPx: number, parentKey: string }>} */
    const revealByKey = new Map();
    if (!Array.isArray(committedSnapshots) || committedSnapshots.length === 0) {
        return { revealByKey, maxRevealTravelPx: 0 };
    }
    const safeRowHeight = Number(rowHeight) > 0 ? Number(rowHeight) : 36;

    /** @type {Map<string, any>} */
    const snapshotByPath = new Map();
    for (let i = 0; i < committedSnapshots.length; i += 1) {
        const snap = committedSnapshots[i];
        if (snap && snap.rowPath) {
            snapshotByPath.set(String(snap.rowPath), snap);
        }
    }

    /** @type {Map<string, { parentSnap: any | null, prevParentTop: number | null, children: Array<any> }>} */
    const groupsByParentKey = new Map();
    for (let i = 0; i < committedSnapshots.length; i += 1) {
        const snap = committedSnapshots[i];
        if (!snap || !snap.rowKey) continue;
        if (prevTopByKey && prevTopByKey.has(snap.rowKey)) continue;
        if (!(snap.depth > 0 && hasRecentlyExpandedAncestor(snap.rowPath))) continue;

        let parentSnap = null;
        let prevParentTop = null;
        if (snap.rowPath && String(snap.rowPath).includes('|||')) {
            const parts = String(snap.rowPath).split('|||');
            // Prefer the deepest ancestor that already existed before this expand so
            // multi-level Expand All unfolds each top-level subtree as one cohesive sheet.
            for (let depth = parts.length - 1; depth >= 1; depth -= 1) {
                const ancestorPath = parts.slice(0, depth).join('|||');
                const candidate = snapshotByPath.get(ancestorPath);
                if (candidate && prevTopByKey && prevTopByKey.has(candidate.rowKey)) {
                    parentSnap = candidate;
                    prevParentTop = prevTopByKey.get(candidate.rowKey) ?? candidate.top;
                    break;
                }
            }
            if (!parentSnap) {
                for (let depth = parts.length - 1; depth >= 1; depth -= 1) {
                    const ancestorPath = parts.slice(0, depth).join('|||');
                    const candidate = snapshotByPath.get(ancestorPath);
                    if (candidate) {
                        parentSnap = candidate;
                        prevParentTop = candidate.top;
                        break;
                    }
                }
            }
        }

        const parentKey = parentSnap ? String(parentSnap.rowKey) : '__viewport__';
        let group = groupsByParentKey.get(parentKey);
        if (!group) {
            group = { parentSnap, prevParentTop, children: [] };
            groupsByParentKey.set(parentKey, group);
        }
        group.children.push(snap);
    }

    let maxRevealTravelPx = 0;
    groupsByParentKey.forEach((group, parentKey) => {
        const { parentSnap, prevParentTop, children } = group;
        if (!children || children.length === 0) return;

        let sumSizes = 0;
        for (let i = 0; i < children.length; i += 1) {
            const childSize = Number(children[i].size);
            sumSizes += childSize > 0 ? childSize : safeRowHeight;
        }

        if (parentSnap && typeof prevParentTop === 'number') {
            const lastChild = children[children.length - 1];
            const lastSize = Number(lastChild.size) > 0 ? Number(lastChild.size) : safeRowHeight;
            const parentSize = Number(parentSnap.size) > 0 ? Number(parentSnap.size) : safeRowHeight;
            const geometricSpan = Math.max(0, (lastChild.top + lastSize) - (parentSnap.top + parentSize));
            const rawSpan = Math.max(sumSizes, geometricSpan);
            const sheetSpan = Math.min(rawSpan, MAX_SHEET_TRAVEL_PX);
            const parentMovedDelta = parentSnap.top - prevParentTop;

            for (let i = 0; i < children.length; i += 1) {
                const child = children[i];
                let fromTop = child.top - sheetSpan - parentMovedDelta;
                const totalTravel = child.top - fromTop;
                if (Math.abs(totalTravel) > MAX_FLIP_DISTANCE_PX) {
                    fromTop = child.top - Math.min(rawSpan, MAX_SHEET_TRAVEL_PX);
                }
                const travelPx = Math.abs(child.top - fromTop);
                if (travelPx > maxRevealTravelPx) maxRevealTravelPx = travelPx;
                revealByKey.set(child.rowKey, {
                    fromTop,
                    toTop: child.top,
                    travelPx,
                    parentKey,
                });
            }
            return;
        }

        // Fallback when parent scrolled above the visible window: gentle short slide-in.
        const fallbackTravel = Math.min(safeRowHeight, 44);
        if (fallbackTravel > maxRevealTravelPx) maxRevealTravelPx = fallbackTravel;
        for (let i = 0; i < children.length; i += 1) {
            const child = children[i];
            revealByKey.set(child.rowKey, {
                fromTop: child.top - fallbackTravel,
                toTop: child.top,
                travelPx: fallbackTravel,
                parentKey,
            });
        }
    });

    return { revealByKey, maxRevealTravelPx };
}

/**
 * Build a smooth top-to-bottom materialize plan for rows arriving after a query
 * (skeleton -> data replacement or initial viewport arrival).
 *
 * - Full-viewport arrivals (initial mount, or structural queries where every visible
 *   slot was a skeleton such as Sort, Filter, Expand All, Collapse All, or field drags)
 *   cascade every visible arriving row uniformly even when rowCount grows.
 * - Partial arrivals (e.g., scrolling into an uncached server block) animate only the
 *   slots that replaced skeletons, leaving surviving cached rows untouched.
 *
 * @param {{
 *   committedSnapshots: Array<any>,
 *   prevSkeletonIndices: Set<number>,
 *   prevTopByKey: Map<string, number>,
 *   expandRevealByKey?: Map<string, any> | null,
 *   isInitialMount?: boolean,
 * }} params
 * @returns {Map<string, { delayMs: number, durationMs: number, offsetYPx: number, order: number }>}
 */
export function buildSkeletonFillPlan({
    committedSnapshots,
    prevSkeletonIndices,
    prevTopByKey,
    expandRevealByKey = null,
    isInitialMount = false,
}) {
    /** @type {Map<string, { delayMs: number, durationMs: number, offsetYPx: number, order: number }>} */
    const fillByKey = new Map();
    if (!Array.isArray(committedSnapshots) || committedSnapshots.length === 0) {
        return fillByKey;
    }
    const hadNoPreviousRows = !prevTopByKey || prevTopByKey.size === 0;
    const hadSkeletons = Boolean(prevSkeletonIndices && prevSkeletonIndices.size > 0);
    const isFullViewportArrival = hadNoPreviousRows && (hadSkeletons || Boolean(isInitialMount));
    if (!isFullViewportArrival && !hadSkeletons) {
        return fillByKey;
    }

    let order = 0;
    for (let i = 0; i < committedSnapshots.length; i += 1) {
        const snap = committedSnapshots[i];
        if (!snap || !snap.rowKey) continue;
        if (prevTopByKey && prevTopByKey.has(snap.rowKey)) continue;
        if (expandRevealByKey && expandRevealByKey.has(snap.rowKey)) continue;
        if (!isFullViewportArrival && !prevSkeletonIndices.has(snap.virtualIndex)) continue;

        const delayMs = Math.min(order * SKELETON_FILL_STAGGER_MS, SKELETON_FILL_MAX_STAGGER_MS);
        fillByKey.set(snap.rowKey, {
            delayMs,
            durationMs: SKELETON_FILL_DURATION_MS,
            offsetYPx: SKELETON_FILL_OFFSET_Y_PX,
            order,
        });
        order += 1;
    }

    return fillByKey;
}
