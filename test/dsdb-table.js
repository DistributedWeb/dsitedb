const dSiteDbTest = require('ava')
const {newDSiteDB, ts} = require('./lib/util')
const DWebVault = require('@dpack/vault')
const tempy = require('tempy')

dSiteDbTest.before(() => console.log('dsdb-table.js'))

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

dSiteDbTest('dSiteDB Tests: count()', async t => {
  var result
  const testDSiteDB = await setupNewDB()
  result = await testDSiteDB.single.count()
  t.is(result, 10)
  result = await testDSiteDB.multi.count()
  t.is(result, 30)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: each()', async t => {
  var n
  var result
  const testDSiteDB = await setupNewDB()
  n = 0
  await testDSiteDB.single.each(result => {
    n++
    t.truthy(result && 'first' in result && 'second' in result && 'third' in result)
  })
  t.is(n, 10)
  n = 0
  await testDSiteDB.multi.each(result => {
    n++
    t.truthy(result && 'first' in result && 'second' in result && 'third' in result)
  })
  t.is(n, 30)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: filter()', async t => {
  const testDSiteDB = await setupNewDB()
  var results = await testDSiteDB.single.filter(r => r.first === 'first5').toArray()
  t.is(results[0].first, 'first5')
  t.is(results.length, 1)
  var results = await testDSiteDB.single
    .filter(r => r.first.startsWith('first'))
    .filter(r => r.second === 5)
    .toArray()
  t.is(results[0].first, 'first5')
  t.is(results.length, 1)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: get()', async t => {
  const testDSiteDB = await setupNewDB()

  var result = await testDSiteDB.single.get(vaults[0].url + '/single.json')
  t.is(result.getRecordURL(), vaults[0].url + '/single.json')
  t.is(result.getRecordOrigin(), vaults[0].url)
  t.is(typeof result.getIndexedAt(), 'number')
  t.truthy(result && 'first' in result && 'second' in result && 'third' in result)
  var result = await testDSiteDB.single.get('first', 'first0')
  t.truthy(result && 'first' in result && 'second' in result && 'third' in result)
  var result = await testDSiteDB.single.get('second', 0)
  t.truthy(result && 'first' in result && 'second' in result && 'third' in result)
  var result = await testDSiteDB.single.get('first+second', ['first0', 0])
  t.truthy(result && 'first' in result && 'second' in result && 'third' in result)
  var result = await testDSiteDB.single.get('first', 'notfound')
  t.falsy(result)

  var result = await testDSiteDB.multi.get(vaults[0].url + '/multi/1.json')
  t.truthy(result && 'first' in result && 'second' in result && 'third' in result)
  var result = await testDSiteDB.multi.get('first', 'first0')
  t.truthy(result && 'first' in result && 'second' in result && 'third' in result)
  var result = await testDSiteDB.multi.get('second', 0)
  t.truthy(result && 'first' in result && 'second' in result && 'third' in result)
  var result = await testDSiteDB.multi.get('first+second', ['first0', 0])
  t.truthy(result && 'first' in result && 'second' in result && 'third' in result)
  var result = await testDSiteDB.multi.get('first', 'notfound')
  t.falsy(result)

  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: offset() and limit()', async t => {
  const testDSiteDB = await setupNewDB()
  var results = await testDSiteDB.single.offset(1).toArray()
  t.is(results.length, 9)
  var results = await testDSiteDB.single.limit(2).toArray()
  t.is(results.length, 2)
  var results = await testDSiteDB.single.offset(1).limit(2).toArray()
  t.is(results.length, 2)
  var results = await testDSiteDB.single.offset(1).limit(2).reverse().toArray()
  t.is(results.length, 2)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: orderBy()', async t => {
  var result
  const testDSiteDB = await setupNewDB()
  result = await testDSiteDB.single.orderBy('first').first()
  t.is(result.first, 'first0')
  result = await testDSiteDB.single.orderBy('second').first()
  t.is(result.second, 0)
  result = await testDSiteDB.single.orderBy('first+second').first()
  t.is(result.first, 'first0')
  t.is(result.second, 0)
  result = await testDSiteDB.single.orderBy('third').first()
  t.is(result.third, 'third0single')
  result = await testDSiteDB.multi.orderBy('first').first()
  t.is(result.first, 'first0')
  result = await testDSiteDB.multi.orderBy('second').first()
  t.is(result.second, 0)
  result = await testDSiteDB.multi.orderBy('first+second').first()
  t.is(result.first, 'first0')
  t.is(result.second, 0)
  result = await testDSiteDB.multi.orderBy('third').first()
  t.is(result.third, 'third0multi1')
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: dSiteDB Tests: reverse()', async t => {
  var result
  const testDSiteDB = await setupNewDB()
  result = await testDSiteDB.single.reverse().toArray()
  t.truthy(result[0].first, 'first9')
  result = await testDSiteDB.multi.reverse().toArray()
  t.truthy(result[0].first, 'first9')
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: dSiteDB Tests: toArray()', async t => {
  var n
  var result
  const testDSiteDB = await setupNewDB()
  n = 0
  var results = await testDSiteDB.single.toArray()
  results.forEach(result => {
    n++
    t.truthy(result && 'first' in result && 'second' in result && 'third' in result)
  })
  t.is(n, 10)
  n = 0
  results = await testDSiteDB.multi.toArray()
  results.forEach(result => {
    n++
    t.truthy(result && 'first' in result && 'second' in result && 'third' in result)
  })
  t.is(n, 30)
  await testDSiteDB.close()
})
