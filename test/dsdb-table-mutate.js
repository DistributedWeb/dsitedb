const dSiteDbTest = require('ava')
const {newDSiteDB, ts} = require('./lib/util')
const {debug} = require('../lib/util')
const DPackVault = require('@dpack/vault')
const tempy = require('tempy')

dSiteDbTest.before(() => console.log('dsdb-table-mutate.js'))

async function setupNewDB () {
  var vaults = []
  async function def (fn) {
    const a = await DPackVault.create({localPath: tempy.directory()})
    await a.mkdir('/multi')
    const write = (path, record) => a.writeFile(path, JSON.stringify(record))
    await fn(write, a)
    return a
  }
  for (let i = 0; i < 10; i++) {
    vaults.push(await def(async write => {
      await write('/single.json', {first: 'first' + i, second: i, third: 'third' + i + 'single'})
      await write(`/multi/first${i}.json`, {first: 'first' + i, second: (i+1)*100, third: 'third' + i + 'multi1'})
      await write(`/multi/first${i}.json`, {first: 'first' + i, second: i, third: 'third' + i + 'multi2'})
      await write(`/multi/first${(i+1)*100}.json`, {first: 'first' + (i+1)*100, second: i, third: 'third' + i + 'multi3'})
    }))
  }

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
  return [vaults, testDSiteDB]
}

dSiteDbTest('dSiteDB Tests: Table.put()', async t => {
  t.plan(14)
  var result
  const [vaults, testDSiteDB] = await setupNewDB()

  // add a multi record
  testDSiteDB.multi.once('put-record', ({url, origin, record}) => {
    t.deepEqual(url, `${vaults[0].url}/multi/4.json`)
    t.deepEqual(origin, vaults[0].url)
    t.deepEqual(record, {
      first: 4,
      second: 'foobar',
      third: 'whoop'
    })
  })
  result = await testDSiteDB.multi.put(vaults[0].url + '/multi/4.json', {
    first: 4,
    second: 'foobar',
    third: 'whoop'
  })
  t.is(result, vaults[0].url + '/multi/4.json')

  // fetch it back
  result = await testDSiteDB.multi.get('first', 4)
  t.is(result.first, 4)
  t.is(result.second, 'foobar')
  t.is(result.third, 'whoop')

  // overwrite the single record
  testDSiteDB.single.once('put-record', ({url, origin, record}) => {
    t.deepEqual(url, `${vaults[0].url}/single.json`)
    t.deepEqual(origin, vaults[0].url)
    t.deepEqual(record, {
      first: 'first100000',
      second: 100000,
      third: 'third100000single'
    })
  })
  result = await testDSiteDB.single.put(vaults[0].url + '/single.json', {
    first: 'first100000',
    second: 100000,
    third: 'third100000single'
  })
  t.is(result, vaults[0].url + '/single.json')

  // fetch it back
  result = await testDSiteDB.single.get(vaults[0].url + '/single.json')
  t.is(result.first, 'first100000')
  t.is(result.second, 100000)
  t.is(result.third, 'third100000single')

  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: Table.delete()', async t => {
  t.plan(7)
  var result
  const [vaults, testDSiteDB] = await setupNewDB()

  // delete a multi record
  testDSiteDB.multi.once('del-record', ({url, origin, record}) => {
    t.deepEqual(url, `${vaults[0].url}/multi/first0.json`)
    t.deepEqual(origin, vaults[0].url)
  })
  result = await testDSiteDB.multi.delete(vaults[0].url + '/multi/first0.json')
  t.is(result, 1)

  // fetch it back
  result = await testDSiteDB.multi.get('first', 'first0')
  t.falsy(result)

  // delete the single record
  testDSiteDB.single.once('del-record', ({url, origin, record}) => {
    t.deepEqual(url, `${vaults[0].url}/single.json`)
    t.deepEqual(origin, vaults[0].url)
  })
  result = await testDSiteDB.single.delete(vaults[0].url + '/single.json')

  // fetch it back
  result = await testDSiteDB.single.get(vaults[0].url + '/single.json')
  t.falsy(result)

  await testDSiteDB.close()
})

dSiteDbTest('Table.update()', async t => {
  const [vaults, testDSiteDB] = await setupNewDB()

  // update a multi record
  var record = await testDSiteDB.multi.get('third', 'third0multi3')
  record.n = 0
  debug('== update by url')
  t.is(await testDSiteDB.multi.update(record.getRecordURL(), {n: 1}), 1)
  t.is((await testDSiteDB.multi.get('third', 'third0multi3')).n, 1)

  // update a single record
  var record = await testDSiteDB.single.query().first()
  record.n = 0
  debug('== update by url')
  t.is(await testDSiteDB.single.update(record.getRecordURL(), {n: 1}), 1)
  t.is((await testDSiteDB.single.get(record.getRecordURL())).n, 1)

  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: Table.upsert() using an object', async t => {
  const [vaults, testDSiteDB] = await setupNewDB()

  // upsert a multi record
  const url = await testDSiteDB.multi.upsert(vaults[0].url + '/multi/5.json', {first: 'upFirst', second: 'upSecond', third: 'upThird'})
  t.is(url, vaults[0].url + '/multi/5.json')
  t.is(await testDSiteDB.multi.upsert(vaults[0].url + '/multi/5.json', {first: 'upFirst', second: 'UPSECOND', third: 'UPTHIRD'}), 1)
  t.is((await testDSiteDB.multi.get('first', 'upFirst')).third, 'UPTHIRD') // this dSiteDbTest data is upthird

  // upsert a single record
  t.is(await testDSiteDB.single.upsert(vaults[0].url + '/single.json', {first: 'upFirst', second: 'upSecond', third: 'upThird'}), 1)
  t.is((await testDSiteDB.single.get('first', 'upFirst')).third, 'upThird')
  t.is(await testDSiteDB.single.upsert(vaults[0].url + '/single.json', {first: 'upFirst', second: 'UPSECOND', third: 'UPTHIRD'}), 1)
  t.is((await testDSiteDB.single.get('first', 'upFirst')).third, 'UPTHIRD')

  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: Table.upsert() using a function', async t => {
  const [vaults, testDSiteDB] = await setupNewDB()

  const updater = record => {
    if (!record) {
      return {first: 'upFirst', second: 'upSecond', third: 'upThird'}
    }
    record.third = record.third.toUpperCase()
    return record
  }

  // upsert a multi record
  const url = await testDSiteDB.multi.upsert(vaults[0].url + '/multi/6.json', updater)
  t.is(url, vaults[0].url + '/multi/6.json')
  t.is(await testDSiteDB.multi.upsert(vaults[0].url + '/multi/6.json', updater), 1)
  t.is((await testDSiteDB.multi.get('first', 'upFirst')).third, 'UPTHIRD')

  // upsert a single record
  t.is(await testDSiteDB.single.upsert(vaults[0].url + '/single.json', updater), 1)
  t.is((await testDSiteDB.single.get('first', 'first0')).third, 'THIRD0SINGLE')

  await testDSiteDB.close()
})
