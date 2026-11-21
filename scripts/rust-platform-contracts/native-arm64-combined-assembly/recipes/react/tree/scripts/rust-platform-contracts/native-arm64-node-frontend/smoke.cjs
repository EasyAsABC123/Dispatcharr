'use strict';
const assert = require('node:assert/strict');
const { createHash } = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');

const root = '/dispatcharr-frontend';
const dist = path.join(root, 'dist');
const stamp = (data) => createHash('sha256').update(data).digest('hex');
function inspectFile(name, file) {
    try {
        return fs.readFileSync(file);
    } catch (error) {
        const code = ['ENOENT', 'EACCES', 'EPERM'].includes(error.code) ? error.code : 'OTHER';
        console.error(`probe-file-error=${name}:${code}`);
        throw error;
    }
}

function checkNativeBinaries() {
    const modules = [
        ['esbuild', 'node_modules/@esbuild/linux-arm64/bin/esbuild'],
        ['rollup', 'node_modules/@rollup/rollup-linux-arm64-gnu/rollup.linux-arm64-gnu.node'],
        ['swc', 'node_modules/@swc/core-linux-arm64-gnu/swc.linux-arm64-gnu.node'],
    ];
    return Object.fromEntries(modules.map(([name, relative]) => {
        const bytes = inspectFile(name, path.join(root, relative));
        assert.equal(bytes.subarray(0, 7).toString('hex'), '7f454c46020101');
        assert.equal(bytes.readUInt16LE(18), 183);
        return [name, stamp(bytes)];
    }));
}

function collect(directory, relative = '') {
    const records = [];
    for (const item of fs.readdirSync(directory, { withFileTypes: true })
        .sort((left, right) => left.name < right.name ? -1 : left.name > right.name ? 1 : 0)) {
        const source = path.join(directory, item.name);
        const name = relative ? `${relative}/${item.name}` : item.name;
        assert.match(name, /^[a-zA-Z0-9_.@/-]{1,200}$/);
        if (item.isDirectory()) {
            records.push(...collect(source, name));
            continue;
        }
        assert(item.isFile());
        const bytes = fs.readFileSync(source);
        assert(bytes.length <= 32 * 1024 * 1024);
        records.push({ path: name, bytes: bytes.length, sha256: stamp(bytes) });
    }
    return records;
}

assert.equal(process.getuid(), 65532);
assert.equal(process.platform, 'linux');
assert.equal(process.arch, 'arm64');
assert.equal(process.version, 'v24.22.2');
const lock = inspectFile('package-lock', path.join(root, 'package-lock.json'));
assert.equal(stamp(lock), '85c702648c56a102931b3577785eb61b54caabdc2f990fc170cbd1666414d4f9');
const credits = Object.fromEntries(['LICENSE', 'ATTRIBUTION.md'].map((name) => [
    name, stamp(inspectFile('credit', path.join('/validation-credit', name))),
]));
assert.deepEqual(credits, {
    'LICENSE': '8486a10c4393cee1c25392769ddd3b2d6c242d6ec7928e1414efff7dfb2f07ef',
    'ATTRIBUTION.md': '1202a089caf31b613a39ac819c8b362cd939cf828328a3880d1ffa16a45ffea7',
});
const assets = collect(dist);
assert(assets.length > 5 && assets.length <= 2000);
const files = new Map(assets.map((row) => [row.path, row]));
assert.equal(files.size, assets.length);
const index = inspectFile('index', path.join(dist, 'index.html')).toString('utf8');
assert(index.includes('id="root"') && index.includes('type="module"'));
const references = [...index.matchAll(/(?:src|href)="(\/assets\/[a-zA-Z0-9_.-]+)"/g)]
    .map((match) => match[1].slice(1));
assert(references.some((name) => name.endsWith('.js')) && references.some((name) => name.endsWith('.css')));
assert(references.every((name) => files.has(name) && files.get(name).bytes > 0));
const sourceMapCount = assets.filter((row) => row.path.endsWith('.map')).length;
assert(sourceMapCount > 0);
const totalBytes = assets.reduce((sum, row) => sum + row.bytes, 0);
assert(totalBytes <= 128 * 1024 * 1024);
console.log(JSON.stringify({
    scope: 'native ARM64 Node production React frontend build',
    arch: process.arch, platform: process.platform, node: process.version, uid: process.getuid(),
    locked_manifest_sha256: stamp(lock), credits, files: assets.length, bytes: totalBytes,
    source_map_count: sourceMapCount, assets_sha256: stamp(JSON.stringify(assets)),
    native_tool_sha256: checkNativeBinaries(), entry_references: references,
    index_sha256: files.get('index.html').sha256, frontend_started: false,
    integrated_package_verified: false, distribution_qualified: false,
}));
