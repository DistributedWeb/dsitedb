const dSiteDbTest = require('ava')
const {newDSiteDB, ts} = require('./lib/util')
const {debug} = require('../lib/util')
const DWebVault = require('@dpack/vault')
const tempy = require('tempy')

dSiteDbTest.before(() => console.log('dsdb-query-mutate.js'))

var vaults = []

async function setupNewDB () {
  const testDSiteDB = newDSiteDB()
  testDSiteDB.define('single', {
    filePattern: '/single.json',
    index: ['first', 'second', 'first+second', 'third']
  })
  testDSiteDB.define('multi', {
    filePattern: '/multi/*.json',
    index: ['first', 'second', 'first+second', 'third']
  })
  await testDSiteDB.open()
  await testDSiteDB.indexVault(vaults)
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
  for (let i = 0; i < 10; i++) {
    vaults.push(await def(async write => {
      await write('/single.json', {first: 'first' + i, second: i, third: 'third' + i + 'single'})
      await write('/multi/1.json', {first: 'first' + i, second: (i+1)*100, third: 'third' + i + 'multi1'})
      await write('/multi/2.json', {first: 'first' + i, second: i, third: 'third' + i + 'multi2'})
      await write('/multi/3.json', {first: 'first' + (i+1)*100, second: i, third: 'third' + i + 'multi3'})
    }))
  }
})

dSiteDbTest('dSiteDB Tests: Query.delete()', async t => {
  var result
  const testDSiteDB = await setupNewDB()

  // delete multi records
  t.is(await testDSiteDB.multi.filter(record => record.second < 5).delete(), 10)
  t.is(await testDSiteDB.multi.count(), 20)

  // delete single records
  t.is(await testDSiteDB.single.filter(record => record.second < 5).delete(), 5)
  t.is(await testDSiteDB.single.count(), 5)

  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: Query.update()', async t => {
  const incrementSecond = record => { record.second++ }
  const testDSiteDB = await setupNewDB()

  // update multi records by object
  t.is(await testDSiteDB.multi.filter(record => record.second >= 5).update({second: -1}), 20)
  t.is(await testDSiteDB.multi.where('second').equals(-1).count(), 20)

  // update multi records by object
  t.is(await testDSiteDB.multi.where('second').equals(-1).update(incrementSecond), 20)
  t.is(await testDSiteDB.multi.where('second').equals(0).count(), 20)

  // update single records by object
  t.is(await testDSiteDB.single.filter(record => record.second >= 5).update({second: -1}), 5)
  t.is(await testDSiteDB.single.where('second').equals(-1).count(), 5)

  // update single records by object
  t.is(await testDSiteDB.single.where('second').equals(-1).update(incrementSecond), 5)
  t.is(await testDSiteDB.single.where('second').equals(0).count(), 5)

  await testDSiteDB.close()
})
