import { useCallback } from 'react';

export function usePersistence(id, persistence, persistence_type) {
  const getStorage = useCallback(() => {
    if (persistence_type === 'local') return window.localStorage;
    if (persistence_type === 'session') return window.sessionStorage;
    return null;
  }, [persistence_type]);

  const load = useCallback((key, defaultValue) => {
    if (!persistence) return defaultValue;
    const storage = getStorage();
    if (!storage) return defaultValue;
    try {
      const saved = storage.getItem(`${id}-${key}`);
      return saved ? JSON.parse(saved) : defaultValue;
    } catch (e) {
      console.warn('Error loading persistence for', key, e);
      return defaultValue;
    }
  }, [id, persistence, getStorage]);

  const save = useCallback((key, value) => {
    if (!persistence) return;
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
