const dSiteDbTest = require('ava')
const {newDSiteDB, ts} = require('./lib/util')
const DPackVault = require('@dpack/vault')
const tempy = require('tempy')

dSiteDbTest.before(() => console.log('dsdb-multi-entry-index.js'))

var vaults = []

async function setupNewDB () {
  const testDSiteDB = newDSiteDB()
  testDSiteDB.define('table', {
    filePattern: '/table/*.json',
    index: ['key', '*fruits']
  })
  await testDSiteDB.open()
  await testDSiteDB.indexVault(vaults)
  return testDSiteDB
}

dSiteDbTest.before('dSiteDB Tests: setup vaults', async () => {
  async function def (fn) {
    const a = await DPackVault.create({localPath: tempy.directory()})
    await a.mkdir('/table')
    const write = (path, record) => a.writeFile(path, JSON.stringify(record))
    await fn(write, a)
    return a
  }
  vaults.push(await def(async write => {
    await write('/table/1.json', {key: 1, fruits: 'apple'})
    await write('/table/2.json', {key: 2, fruits: ['apple', 'banana']})
    await write('/table/3.json', {key: 3, fruits: ['apple', 'banana', 'cherry']})
  }))
  vaults.push(await def(async write => {
    await write('/table/1.json', {key: 1, fruits: 'cherry'})
    await write('/table/2.json', {key: 2, fruits: ['apple', 'banana']})
  }))
})

dSiteDbTest('dSiteDB Tests: above()', async t => {
  const testDSiteDB = await setupNewDB()
  var results = await testDSiteDB.table.where('fruits').above('banana').toArray()
  t.is(results.length, 2)
  t.is(results.length, results.filter(v => v.fruits === 'cherry' || v.fruits.indexOf('cherry') >= 0).length)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: aboveOrEqual()', async t => {
  const testDSiteDB = await setupNewDB()
  var results = await testDSiteDB.table.where('fruits').aboveOrEqual('banana').toArray()
  t.truthy(results.length > 0)
  t.is(results.filter(v => v.fruits === 'apple').length, 0)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: anyOf()', async t => {
  const testDSiteDB = await setupNewDB()
  var results = await testDSiteDB.table.where('fruits').anyOf('banana', 'cherry').toArray()
  t.truthy(results.length > 0)
  t.is(results.filter(v => v.fruits === 'apple').length, 0)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: anyOfIgnoreCase()', async t => {
  const testDSiteDB = await setupNewDB()
  var results = await testDSiteDB.table.where('fruits').anyOfIgnoreCase('BANANA', 'CHERRY').toArray()
  t.truthy(results.length > 0)
  t.is(results.filter(v => v.fruits === 'apple').length, 0)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: below()', async t => {
  const testDSiteDB = await setupNewDB()
  var results = await testDSiteDB.table.where('fruits').below('banana').toArray()
  t.is(results.length, 4)
  t.is(results.length, results.filter(v => v.fruits === 'apple' || v.fruits.indexOf('apple') >= 0).length)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: belowOrEqual()', async t => {
  const testDSiteDB = await setupNewDB()
  var results = await testDSiteDB.table.where('fruits').belowOrEqual('banana').toArray()
  t.truthy(results.length > 0)
  t.is(results.length, results.filter(v => v.fruits === 'apple' || v.fruits.indexOf('apple') >= 0).length)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: between()', async t => {
  const testDSiteDB = await setupNewDB()
  var results = await testDSiteDB.table.where('fruits').between('apple', 'cherry').toArray()
  t.truthy(results.length > 0)
  t.is(results.length, results.filter(v => v.fruits === 'banana' || v.fruits.indexOf('banana') >= 0).length)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: equals()', async t => {
  const testDSiteDB = await setupNewDB()
  var results = await testDSiteDB.table.where('fruits').equals('banana').toArray()
  t.truthy(results.length > 0)
  t.is(results.length, results.filter(v => v.fruits === 'banana' || v.fruits.indexOf('banana') >= 0).length)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: equalsIgnoreCase()', async t => {
  const testDSiteDB = await setupNewDB()
  var results = await testDSiteDB.table.where('fruits').equalsIgnoreCase('BANANA').toArray()
  t.truthy(results.length > 0)
  t.is(results.length, results.filter(v => v.fruits === 'banana' || v.fruits.indexOf('banana') >= 0).length)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: noneOf()', async t => {
  const testDSiteDB = await setupNewDB()
  var results = await testDSiteDB.table.where('fruits').noneOf('apple', 'cherry').toArray()
  t.truthy(results.length > 0)
  t.is(results.length, results.filter(v => v.fruits === 'banana' || v.fruits.indexOf('banana') >= 0).length)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: notEqual()', async t => {
  const testDSiteDB = await setupNewDB()
  var results = await testDSiteDB.table.where('fruits').notEqual('cherry').toArray()
  t.truthy(results.length > 0)
  t.is(0, results.filter(v => v.fruits === 'cherry').length)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: startsWith()', async t => {
  const testDSiteDB = await setupNewDB()
  var results = await testDSiteDB.table.where('fruits').startsWith('banan').toArray()
  t.truthy(results.length > 0)
  t.is(results.length, results.filter(v => v.fruits === 'banana' || v.fruits.indexOf('banana') >= 0).length)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: startsWithAnyOf()', async t => {
  const testDSiteDB = await setupNewDB()
  var results = await testDSiteDB.table.where('fruits').startsWithAnyOf('banan', 'cherr').toArray()
  t.truthy(results.length > 0)
  t.is(results.filter(v => v.fruits === 'apple').length, 0)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: startsWithAnyOfIgnoreCase()', async t => {
  const testDSiteDB = await setupNewDB()
  var results = await testDSiteDB.table.where('fruits').startsWithAnyOfIgnoreCase('BANAN', 'CHERR').toArray()
  t.truthy(results.length > 0)
  t.is(results.filter(v => v.fruits === 'apple').length, 0)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: startsWithIgnoreCase()', async t => {
  const testDSiteDB = await setupNewDB()
  var results = await testDSiteDB.table.where('fruits').startsWithIgnoreCase('BANAN').toArray()
  t.truthy(results.length > 0)
  t.is(results.length, results.filter(v => v.fruits === 'banana' || v.fruits.indexOf('banana') >= 0).length)
  await testDSiteDB.close()
})
