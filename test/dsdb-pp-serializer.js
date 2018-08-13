const dSiteDbTest = require('ava')
const {newDSiteDB, ts} = require('./lib/util')
const DWebVault = require('@dpack/vault')
const tempy = require('tempy')
const Ajv = require('ajv')

dSiteDbTest.before(() => console.log('dsdb-pp-serializer.js'))

var vault

async function setupNewDB () {
  const testDSiteDB = newDSiteDB()
  testDSiteDB.define('multi', {
    filePattern: ['/multi/*.json'],
    validate: (new Ajv()).compile({
      type: 'object',
      properties: {
        fileAttr: {type: 'string'}
      },
      required: ['fileAttr']
    }),
    preprocess: record => ({
      recordOnly: record.fileAttr + 'record',
      fileAttr: record.fileAttr
    }),
    serialize: record => ({fileAttr: record.fileAttr})
  })
  await testDSiteDB.open()
  await testDSiteDB.indexVault(vault)
  return testDSiteDB
}

dSiteDbTest.before('dSiteDB Tests: setup vaults', async () => {
  async function def (fn) {
    const a = await DWebVault.create({localPath: tempy.directory()})
    await a.mkdir('/multi')
    const write = (path, record) => a.writeFile(path, JSON.stringify(record))
    await fn(write, a)
    return a
  }
  vault = await def(async write => {
    await write('/multi/1.json', {fileAttr: 'foo'})
    await write('/multi/2.json', {fileAttr: 'bar'})
    await write('/multi/3.json', {fileAttr: 'baz'})
  })
})

dSiteDbTest('dSiteDB Tests: Different data is stored in the records than in the files', async t => {
  const testDSiteDB = await setupNewDB()
  // check records
  var results = await testDSiteDB.multi.toArray()
  t.deepEqual(results[0].fileAttr, 'foo')
  t.deepEqual(results[0].recordOnly, 'foorecord')
  t.deepEqual(results[1].fileAttr, 'bar')
  t.deepEqual(results[1].recordOnly, 'barrecord')
  t.deepEqual(results[2].fileAttr, 'baz')
  t.deepEqual(results[2].recordOnly, 'bazrecord')
  // write each record
  await testDSiteDB.multi.put(results[0].getRecordURL(), results[0])
  await testDSiteDB.multi.put(results[1].getRecordURL(), results[1])
  await testDSiteDB.multi.put(results[2].getRecordURL(), results[2])
  // check files
  var files = await Promise.all([
    vault.readFile('/multi/1.json'),
    vault.readFile('/multi/2.json'),
    vault.readFile('/multi/3.json'),
  ])
  files = files.map(JSON.parse)
  t.deepEqual(files[0].fileAttr, 'foo')
  t.deepEqual(files[0].recordOnly, undefined)
  t.deepEqual(files[1].fileAttr, 'bar')
  t.deepEqual(files[1].recordOnly, undefined)
  t.deepEqual(files[2].fileAttr, 'baz')
  t.deepEqual(files[2].recordOnly, undefined)
  await testDSiteDB.close()
})
