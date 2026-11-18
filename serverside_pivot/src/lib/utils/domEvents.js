/** Document listeners receive a retargeted target for events inside a shadow root.  @param {any} [event] @param {any} [element] @returns {any} */
export const isEventInsideElement = (event, element) => Boolean(element && (
    element.contains(event.target)
    || (typeof event.composedPath === 'function' && event.composedPath().includes(element))
));

/** Shadow-DOM-safe check for whether an event originated inside any element matching `selector`. @param {any} event @param {string} selector @returns {boolean} */
export const isEventInsideSelector = (event, selector) => {
    if (!event || !selector) return false;
    if (typeof event.composedPath === 'function') {
        const path = event.composedPath();
        if (Array.isArray(path)) {
            return path.some((node) => Boolean(node && typeof node.matches === 'function' && node.matches(selector)));
        }
    }
    return Boolean(event.target && typeof event.target.closest === 'function' && event.target.closest(selector));
};
