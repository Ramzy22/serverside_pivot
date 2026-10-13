import assert from 'node:assert/strict';
import { test } from 'node:test';
import { ModuleRegistry } from 'ag-grid-community';
import { FiltersToolPanelModule } from 'ag-grid-enterprise';
import { registerReferenceModules } from '../src/modules.ts';

test('the configured Filters side panel module is passed to the real AG Grid registry', () => {
  const original = ModuleRegistry.registerModules;
  let passed: Parameters<typeof ModuleRegistry.registerModules>[0] = [];
  ModuleRegistry.registerModules = (modules) => {
    passed = modules;
    original.call(ModuleRegistry, modules);
  };
  try {
    registerReferenceModules();
  } finally {
    ModuleRegistry.registerModules = original;
  }
  assert.ok(passed.includes(FiltersToolPanelModule));
  assert.ok(passed.some((module) => module.moduleName === FiltersToolPanelModule.moduleName));
});
