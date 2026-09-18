import { useCallback } from 'react';

import {
  createChartDefinitionId,
  normalizeChartDefinition,
  sanitizeChartDefinitionName,
} from '../hooks/usePivotNormalization';

/** Owns create, duplicate, delete, and rename commands for chart definitions. */
export function useRuntimeChartDefinitionCommands({
  activeChartDefinitionId,
  currentChartDefinition,
  initialChartDefinition,
  managedChartDefinitions,
  setActiveChartDefinitionId,
  setManagedChartDefinitions,
}) {
  const handleCreateChartDefinition = useCallback(() => {
    const nextDefinition = normalizeChartDefinition({
      ...currentChartDefinition,
      id: createChartDefinitionId('chart'),
      name: `Chart ${managedChartDefinitions.length + 1}`
    }, initialChartDefinition);
    setManagedChartDefinitions(previousDefinitions => [...previousDefinitions, nextDefinition]);
    setActiveChartDefinitionId(nextDefinition.id);
  }, [currentChartDefinition, initialChartDefinition, managedChartDefinitions.length, setActiveChartDefinitionId, setManagedChartDefinitions]);

  const handleDuplicateChartDefinition = useCallback(() => {
    const sourceDefinition = managedChartDefinitions.find(definition => definition.id === activeChartDefinitionId) || currentChartDefinition;
    const nextDefinition = normalizeChartDefinition({
      ...sourceDefinition,
      id: createChartDefinitionId('chart'),
      name: `${sourceDefinition.name || 'Chart'} Copy`
    }, initialChartDefinition);
    setManagedChartDefinitions(previousDefinitions => [...previousDefinitions, nextDefinition]);
    setActiveChartDefinitionId(nextDefinition.id);
  }, [activeChartDefinitionId, currentChartDefinition, initialChartDefinition, managedChartDefinitions, setActiveChartDefinitionId, setManagedChartDefinitions]);

  const handleDeleteChartDefinition = useCallback(() => {
    setManagedChartDefinitions(previousDefinitions => {
      if (!Array.isArray(previousDefinitions) || previousDefinitions.length <= 1) return previousDefinitions;
      const nextDefinitions = previousDefinitions.filter(definition => definition.id !== activeChartDefinitionId);
      if (nextDefinitions.length > 0) setActiveChartDefinitionId(nextDefinitions[0].id);
      return nextDefinitions;
    });
  }, [activeChartDefinitionId, setActiveChartDefinitionId, setManagedChartDefinitions]);

  const handleRenameChartDefinition = useCallback(name => {
    setManagedChartDefinitions(previousDefinitions => previousDefinitions.map(definition => definition.id === activeChartDefinitionId ? {
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
