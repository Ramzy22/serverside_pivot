/** Document listeners receive a retargeted target for events inside a shadow root. */
export const isEventInsideElement = (event, element) => Boolean(element && (
    element.contains(event.target)
    || (typeof event.composedPath === 'function' && event.composedPath().includes(element))
));
