import { fileURLToPath } from 'node:url';
import path from 'node:path';
import ts from 'typescript';

const projectDir = path.resolve(fileURLToPath(new URL('..', import.meta.url)));
const configPath = path.join(projectDir, 'tsconfig.checked-js.json');
const expectedRoots = [
  'src/lib/runtime/runtimeResponseDedup.js',
  'src/lib/utils/formatting.js',
  'src/lib/utils/Icons.js',
  'src/lib/runtime/useRuntimeExportState.js',
  'src/lib/hooks/useCellInteraction.js',
  'src/lib/utils/rawTableTsv.js',
].map((relativePath) => path.resolve(projectDir, relativePath));

const loaded = ts.readConfigFile(configPath, ts.sys.readFile);
if (loaded.error) {
  throw new Error(`Cannot load checked-JS project: ${ts.flattenDiagnosticMessageText(loaded.error.messageText, '\n')}`);
}
const config = ts.parseJsonConfigFileContent(loaded.config, ts.sys, projectDir, undefined, configPath);
if (config.errors.length > 0) {
  throw new Error(`Invalid checked-JS project: ${config.errors.map((diagnostic) => ts.flattenDiagnosticMessageText(diagnostic.messageText, '\n')).join('; ')}`);
}

const normalize = (filename) => path.normalize(filename).toLowerCase();
const actualRoots = config.fileNames.map(normalize).sort();
const requiredRoots = expectedRoots.map(normalize).sort();
if (actualRoots.length !== requiredRoots.length || actualRoots.some((filename, index) => filename !== requiredRoots[index])) {
  throw new Error(`Checked-JS root files differ from the approved six-file scope: ${config.fileNames.join(', ')}`);
}
if (!config.options.allowJs || !config.options.checkJs || !config.options.strict || !config.options.noEmit) {
  throw new Error('Checked-JS project must enable allowJs, checkJs, strict, and noEmit.');
}

const probePath = path.resolve(projectDir, 'src/lib/utils/formatting.js');
const host = ts.createCompilerHost(config.options);
const readFile = host.readFile.bind(host);
host.readFile = (filename) => {
  const contents = readFile(filename);
  if (contents !== undefined && normalize(filename) === normalize(probePath)) {
    return `${contents}\n/** @type {number} */ const __pivotJsGateProbe = 'bad';\n`;
  }
  return contents;
};
const program = ts.createProgram({ rootNames: config.fileNames, options: config.options, host });
const catchesTypeError = ts.getPreEmitDiagnostics(program).some(
  (diagnostic) => diagnostic.code === 2322 && diagnostic.file && normalize(diagnostic.file.fileName) === normalize(probePath),
);
if (!catchesTypeError) {
  throw new Error('Checked-JS project failed to reject an injected JS type error.');
}
console.log('Checked-JS gate rejects an injected TS2322 in an admitted module.');
