import { useCallback } from 'react';

/** @param {any} [explicitView] @param {any} [savedView] @param {any} [tableName] @returns {any} */
export function resolvePersistedViewState(explicitView, savedView, tableName) {
  if (explicitView !== null && explicitView !== undefined) return explicitView;
  if (!savedView || Array.isArray(savedView) || savedView.version !== 1 ||
      savedView.table !== (tableName || null) || !savedView.state ||
      typeof savedView.state !== 'object' || Array.isArray(savedView.state)) return null;
  return savedView;
}

/** @param {any} [id] @param {any} [persistence] @param {any} [persistence_type] @returns {any} */
export function usePersistence(id, persistence, persistence_type) {
  /** @type {(...args: any[]) => any} */
  const getStorage = useCallback(() => {
    if (persistence_type === 'local') return window.localStorage;
    if (persistence_type === 'session') return window.sessionStorage;
    return null;
  }, [persistence_type]);

  /** @type {(...args: any[]) => any} */
  const load = useCallback((key, defaultValue) => {
    if (!persistence) return defaultValue;
    /** @type {any} */
    const storage = getStorage();
    if (!storage) return defaultValue;
    try {
      /** @type {any} */
      const saved = storage.getItem(`${id}-${key}`);
      return saved ? JSON.parse(saved) : defaultValue;
    } catch (e) {
      console.warn('Error loading persistence for', key, e);
      return defaultValue;
    }
  }, [id, persistence, getStorage]);

  /** @type {(...args: any[]) => any} */
  const save = useCallback((key, value) => {
    if (!persistence) return;
    /** @type {any} */
    const storage = getStorage();
    if (!storage) return;
    try {
      storage.setItem(`${id}-${key}`, JSON.stringify(value));
    } catch (e) {
      console.warn('Error saving persistence for', key, e);
    }
  }, [id, persistence, getStorage]);

  return { load, save };
}
