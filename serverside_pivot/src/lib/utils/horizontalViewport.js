const LARGE_CENTER_COLUMN_COUNT = 5000;
const EXTREME_CENTER_COLUMN_COUNT = 20000;

/**
 * Estimate a safe center-column range for a large horizontal scroll jump.
 *
 * `scrollLeft` is already expressed in the center-column coordinate space:
 * the pinned rails are sticky overlays, while the center track begins at the
 * scroll container's origin.  Applying a pinned-width offset here would make
 * a jump under-fetch visible columns whenever the left rail is wide.
  @param {any} p0 @returns {any} */
export const estimateUrgentHorizontalColumnRange = ({
    scrollLeft,
    clientWidth,
    scrollWidth,
    averageColumnWidth,
    centerColumnCount,
}) => {
    /** @type {any} */
    const safeScrollLeft = Number(scrollLeft);
    /** @type {any} */
    const safeClientWidth = Number(clientWidth);
    /** @type {any} */
    const safeScrollWidth = Number(scrollWidth);
    /** @type {any} */
    const safeCenterColumnCount = Number(centerColumnCount);
    if (
        !Number.isFinite(safeScrollLeft)
        || !Number.isFinite(safeClientWidth)
        || !Number.isFinite(safeScrollWidth)
        || !Number.isFinite(safeCenterColumnCount)
    ) {
        return null;
    }

    /** @type {any} */
    const count = Math.max(0, Math.floor(safeCenterColumnCount));
    if (count <= 0) return null;

    /** @type {any} */
    const averageWidth = Math.max(48, Number.isFinite(Number(averageColumnWidth))
        ? Number(averageColumnWidth)
        : 140);
    /** @type {any} */
    const maxIndex = count - 1;
    /** @type {any} */
    const atRightEdge = (safeScrollLeft + safeClientWidth) >= (safeScrollWidth - 1);
    /** @type {any} */
    const visibleColumnCount = Math.max(1, Math.ceil(safeClientWidth / averageWidth));
    /** @type {any} */
    const largeColumnMode = count >= LARGE_CENTER_COLUMN_COUNT;
    /** @type {any} */
    const extremeColumnMode = count >= EXTREME_CENTER_COLUMN_COUNT;
    /** @type {any} */
    const bufferCount = extremeColumnMode
        ? 1
        : (largeColumnMode
            ? Math.max(1, Math.min(3, Math.ceil(visibleColumnCount / 5)))
            : Math.max(2, Math.min(8, Math.ceil(visibleColumnCount / 3))));
    /** @type {any} */
    const edgeSafetyCount = atRightEdge
        ? (extremeColumnMode
            ? Math.max(bufferCount, Math.min(8, visibleColumnCount))
            : (largeColumnMode
                ? Math.max(bufferCount, Math.min(12, Math.ceil(visibleColumnCount * 1.5)))
                : Math.max(bufferCount, Math.min(24, visibleColumnCount * 2))))
        : bufferCount;

    /** @type {any} */
    let start = Math.max(
        0,
        Math.min(maxIndex, Math.floor(Math.max(0, safeScrollLeft) / averageWidth))
    );
    /** @type {any} */
    let end = Math.min(maxIndex, start + visibleColumnCount + bufferCount);
    start = Math.max(0, start - bufferCount);

    if (atRightEdge) {
        end = maxIndex;
        start = Math.max(0, end - visibleColumnCount - edgeSafetyCount);
    }

    return {
        start,
        end,
        atRightEdge,
        visibleColumnCount,
    };
};
