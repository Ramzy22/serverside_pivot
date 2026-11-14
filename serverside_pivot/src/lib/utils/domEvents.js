/** Document listeners receive a retargeted target for events inside a shadow root.  @param {any} [event] @param {any} [element] @returns {any} */
export const isEventInsideElement = (event, element) => Boolean(element && (
    element.contains(event.target)
    || (typeof event.composedPath === 'function' && event.composedPath().includes(element))
));
