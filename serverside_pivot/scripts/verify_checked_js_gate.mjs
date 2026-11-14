import { fileURLToPath } from 'node:url';
import fs from 'node:fs';
import path from 'node:path';
import ts from 'typescript';

const projectDir = path.resolve(fileURLToPath(new URL('..', import.meta.url)));
const srcLibDir = path.join(projectDir, 'src', 'lib');
const configPath = path.join(projectDir, 'tsconfig.checked-js.json');

function collectSourceFiles(dir, extensionFilter) {
  /** @type {string[]} */
  const out = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      out.push(...collectSourceFiles(fullPath, extensionFilter));
    } else if (extensionFilter(entry.name)) {
      out.push(fullPath);
    }
  }
  return out;
}

const expectedJsFiles = collectSourceFiles(srcLibDir, (name) => name.endsWith('.js'));

for (const filePath of expectedJsFiles) {
  const sourceText = fs.readFileSync(filePath, 'utf8');
  if (/@ts-nocheck\b/.test(sourceText)) {
    throw new Error(`Blanket @ts-nocheck suppression is forbidden in ${path.relative(projectDir, filePath)}`);
  }
}

const loaded = ts.readConfigFile(configPath, ts.sys.readFile);
if (loaded.error) {
  throw new Error(`Cannot load checked-JS project: ${ts.flattenDiagnosticMessageText(loaded.error.messageText, '\n')}`);
}
const config = ts.parseJsonConfigFileContent(loaded.config, ts.sys, projectDir, undefined, configPath);
if (config.errors.length > 0) {
  throw new Error(`Invalid checked-JS project: ${config.errors.map((diagnostic) => ts.flattenDiagnosticMessageText(diagnostic.messageText, '\n')).join('; ')}`);
}

const normalize = (filename) => path.normalize(filename).toLowerCase();
const actualFileSet = new Set(config.fileNames.map(normalize));
const missingJsFiles = expectedJsFiles.filter((filePath) => !actualFileSet.has(normalize(filePath)));
if (missingJsFiles.length > 0) {
  throw new Error(`Checked-JS project is missing ${missingJsFiles.length} JS file(s): ${missingJsFiles.join(', ')}`);
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
const probeSourceFile = program.getSourceFile(probePath) || program.getSourceFiles().find((sf) => normalize(sf.fileName) === normalize(probePath));
const catchesTypeError = ts.getPreEmitDiagnostics(program, probeSourceFile).some(
  (diagnostic) => diagnostic.code === 2322 && diagnostic.file && normalize(diagnostic.file.fileName) === normalize(probePath),
);
if (!catchesTypeError) {
  throw new Error('Checked-JS project failed to reject an injected JS type error.');
}
console.log(`Checked-JS gate verified all ${expectedJsFiles.length} JS modules and rejected an injected TS2322.`);
