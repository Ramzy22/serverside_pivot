import assert from 'node:assert/strict';
import { test } from 'node:test';
import { ColumnAutoSizeModule, ModuleRegistry } from 'ag-grid-community';
import { FiltersToolPanelModule, GroupFilterModule } from 'ag-grid-enterprise';
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
  assert.ok(passed.includes(GroupFilterModule), 'Grouped hidden fields must have a real header-filter UI');
  assert.ok(passed.includes(ColumnAutoSizeModule), 'Header-edge double-click auto-size requires its real sizing module');
});
