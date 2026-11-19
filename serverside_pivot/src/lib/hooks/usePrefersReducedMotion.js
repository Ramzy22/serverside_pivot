import { useEffect, useState } from 'react';

const REDUCED_MOTION_QUERY = '(prefers-reduced-motion: reduce)';

/**
 * Returns true when the user has requested reduced motion at the OS/browser level.
 * SSR- and JSDOM-safe: returns false when `window.matchMedia` is unavailable.
 *
 * @returns {boolean}
 */
export function usePrefersReducedMotion() {
    const [reducedMotion, setReducedMotion] = useState(() => {
        if (typeof window === 'undefined' || typeof window.matchMedia !== 'function') {
            return false;
        }
        try {
            return Boolean(window.matchMedia(REDUCED_MOTION_QUERY).matches);
        } catch (_err) {
            return false;
        }
    });

    useEffect(() => {
        if (typeof window === 'undefined' || typeof window.matchMedia !== 'function') {
            return undefined;
        }
        /** @type {MediaQueryList | null} */
        let mediaQuery = null;
        try {
            mediaQuery = window.matchMedia(REDUCED_MOTION_QUERY);
        } catch (_err) {
            return undefined;
        }
        if (!mediaQuery) return undefined;

        setReducedMotion(Boolean(mediaQuery.matches));
        /** @param {MediaQueryListEvent} event */
        const handleChange = (event) => {
            setReducedMotion(Boolean(event.matches));
        };

        if (typeof mediaQuery.addEventListener === 'function') {
            mediaQuery.addEventListener('change', handleChange);
            return () => mediaQuery && mediaQuery.removeEventListener('change', handleChange);
        }
        if (typeof mediaQuery.addListener === 'function') {
            mediaQuery.addListener(handleChange);
            return () => mediaQuery && mediaQuery.removeListener(handleChange);
        }
        return undefined;
    }, []);

    return reducedMotion;
}

export default usePrefersReducedMotion;
