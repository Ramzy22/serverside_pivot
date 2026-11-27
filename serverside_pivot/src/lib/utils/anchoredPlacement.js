/**
 * Placement for a popover anchored to an element.
 *
 * The popover opens below the anchor and caps its height to the space there.
 * It opens above only when the space below is less than `comfortableHeight`
 * (or the content, if shorter) and there is more room above.
 *
 * @param {{ top: number, bottom: number, left: number }} rect anchor bounds (viewport coordinates)
 * @param {{
 *   width: number,
 *   contentHeight?: number,
 *   viewportWidth: number,
 *   viewportHeight: number,
 *   gap?: number,
 *   margin?: number,
 *   comfortableHeight?: number,
 *   maxHeightCap?: number,
 * }} options
 * @returns {{ top: number, left: number, maxHeight: number, placement: 'below' | 'above' }}
 */
export const computeAnchoredPlacement = (rect, {
    width,
    contentHeight,
    viewportWidth,
    viewportHeight,
    gap = 4,
    margin = 12,
    comfortableHeight = 240,
    maxHeightCap = Infinity,
}) => {
    const spaceBelow = Math.max(0, viewportHeight - rect.bottom - gap - margin);
    const spaceAbove = Math.max(0, rect.top - gap - margin);
    const needed = Number.isFinite(contentHeight) && /** @type {number} */ (contentHeight) > 0
        ? Math.min(/** @type {number} */ (contentHeight), comfortableHeight)
        : comfortableHeight;
    const placement = spaceBelow < needed && spaceAbove > spaceBelow ? 'above' : 'below';
    const maxHeight = Math.floor(Math.min(maxHeightCap, placement === 'above' ? spaceAbove : spaceBelow));
    const renderedHeight = Number.isFinite(contentHeight) && /** @type {number} */ (contentHeight) > 0
        ? Math.min(/** @type {number} */ (contentHeight), maxHeight)
        : maxHeight;
    const top = placement === 'above'
        ? Math.max(margin, rect.top - gap - renderedHeight)
        : rect.bottom + gap;
    const left = Math.max(margin, Math.min(rect.left, viewportWidth - width - margin));
    return { top, left, maxHeight, placement };
};
