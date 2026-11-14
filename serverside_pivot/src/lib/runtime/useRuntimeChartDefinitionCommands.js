import { useCallback } from 'react';

import {
  createChartDefinitionId,
  normalizeChartDefinition,
  sanitizeChartDefinitionName,
} from '../hooks/usePivotNormalization';

/** Owns create, duplicate, delete, and rename commands for chart definitions.  @param {any} p0 @returns {any} */
export function useRuntimeChartDefinitionCommands({
  activeChartDefinitionId,
  currentChartDefinition,
  initialChartDefinition,
  managedChartDefinitions,
  setActiveChartDefinitionId,
  setManagedChartDefinitions,
}) {
  /** @type {(...args: any[]) => any} */
  const handleCreateChartDefinition = useCallback(() => {
    /** @type {any} */
    const nextDefinition = normalizeChartDefinition({
      ...currentChartDefinition,
      id: createChartDefinitionId('chart'),
      name: `Chart ${managedChartDefinitions.length + 1}`
    }, initialChartDefinition);
    setManagedChartDefinitions(/** @param {any} [previousDefinitions] @returns {any} */ previousDefinitions => [...previousDefinitions, nextDefinition]);
    setActiveChartDefinitionId(nextDefinition.id);
  }, [currentChartDefinition, initialChartDefinition, managedChartDefinitions.length, setActiveChartDefinitionId, setManagedChartDefinitions]);

  /** @type {(...args: any[]) => any} */
  const handleDuplicateChartDefinition = useCallback(() => {
    /** @type {any} */
    const sourceDefinition = managedChartDefinitions.find(/** @param {any} [definition] @returns {any} */ definition => definition.id === activeChartDefinitionId) || currentChartDefinition;
    /** @type {any} */
    const nextDefinition = normalizeChartDefinition({
      ...sourceDefinition,
      id: createChartDefinitionId('chart'),
      name: `${sourceDefinition.name || 'Chart'} Copy`
    }, initialChartDefinition);
    setManagedChartDefinitions(/** @param {any} [previousDefinitions] @returns {any} */ previousDefinitions => [...previousDefinitions, nextDefinition]);
    setActiveChartDefinitionId(nextDefinition.id);
  }, [activeChartDefinitionId, currentChartDefinition, initialChartDefinition, managedChartDefinitions, setActiveChartDefinitionId, setManagedChartDefinitions]);

  /** @type {(...args: any[]) => any} */
  const handleDeleteChartDefinition = useCallback(() => {
    setManagedChartDefinitions(/** @param {any} [previousDefinitions] @returns {any} */ previousDefinitions => {
      if (!Array.isArray(previousDefinitions) || previousDefinitions.length <= 1) return previousDefinitions;
      /** @type {any} */
      const nextDefinitions = previousDefinitions.filter(definition => definition.id !== activeChartDefinitionId);
      if (nextDefinitions.length > 0) setActiveChartDefinitionId(nextDefinitions[0].id);
      return nextDefinitions;
    });
  }, [activeChartDefinitionId, setActiveChartDefinitionId, setManagedChartDefinitions]);

  /** @type {(...args: any[]) => any} */
  const handleRenameChartDefinition = useCallback(name => {
    setManagedChartDefinitions(/** @param {any} [previousDefinitions] @returns {any} */ previousDefinitions => previousDefinitions.map(/** @param {any} [definition] @returns {any} */ definition => definition.id === activeChartDefinitionId ? {
      ...definition,
      name: sanitizeChartDefinitionName(name, definition.name || 'Chart')
    } : definition));
  }, [activeChartDefinitionId, setManagedChartDefinitions]);

  return {
    handleCreateChartDefinition,
    handleDeleteChartDefinition,
    handleDuplicateChartDefinition,
    handleRenameChartDefinition,
  };
}
