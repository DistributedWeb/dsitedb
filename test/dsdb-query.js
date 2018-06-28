const dSiteDbTest = require('ava')
const {newDSiteDB, ts} = require('./lib/util')
const DPackVault = require('@dpack/vault')
const tempy = require('tempy')

dSiteDbTest.before(() => console.log('dsdb-query.js'))

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
    const a = await DPackVault.create({localPath: tempy.directory()})
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

dSiteDbTest('each()', async t => {
  var n
  var result
  const testDSiteDB = await setupNewDB()
  n = 0
  await testDSiteDB.single.query().each(result => {
    n++
    t.truthy(result && 'first' in result && 'second' in result && 'third' in result)
  })
  t.is(n, 10)
  n = 0
  await testDSiteDB.multi.query().each(result => {
    n++
    t.truthy(result && 'first' in result && 'second' in result && 'third' in result)
  })
  t.is(n, 30)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: toArray()', async t => {
  var n
  var result
  const testDSiteDB = await setupNewDB()
  n = 0
  var results = await testDSiteDB.single.query().toArray()
  results.forEach(result => {
    n++
    t.truthy(result && 'first' in result && 'second' in result && 'third' in result)
  })
  t.is(n, 10)
  n = 0
  results = await testDSiteDB.multi.query().toArray()
  results.forEach(result => {
    n++
    t.truthy(result && 'first' in result && 'second' in result && 'third' in result)
  })
  t.is(n, 30)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: eachKey()', async t => {
  var n
  var result
  const testDSiteDB = await setupNewDB()
  n = 0
  await testDSiteDB.single.query().eachKey(result => {
    n++
    t.truthy(typeof result === 'string')
    // is .url
    t.truthy(result.startsWith('dweb://'))
    t.truthy(result.endsWith('.json'))
  })
  t.is(n, 10)
  n = 0
  await testDSiteDB.multi.query().eachKey(result => {
    n++
    t.truthy(typeof result === 'string')
    // is .url
    t.truthy(result.startsWith('dweb://'))
    t.truthy(result.endsWith('.json'))
  })
  t.is(n, 30)
  n = 0
  await testDSiteDB.multi.orderBy('second').eachKey(result => {
    n++
    // is .second
    t.truthy(typeof result === 'number')
  })
  t.is(n, 30)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: keys()', async t => {
  var n
  var result
  const testDSiteDB = await setupNewDB()
  n = 0
  var keys = await testDSiteDB.single.query().keys()
  keys.forEach(result => {
    n++
    t.truthy(typeof result === 'string')
    // is .url
    t.truthy(result.startsWith('dweb://'))
    t.truthy(result.endsWith('.json'))
  })
  t.is(n, 10)
  n = 0
  var keys = await testDSiteDB.multi.query().keys()
  keys.forEach(result => {
    n++
    t.truthy(typeof result === 'string')
    // is .url
    t.truthy(result.startsWith('dweb://'))
    t.truthy(result.endsWith('.json'))
  })
  t.is(n, 30)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: eachUrl()', async t => {
  var n
  var result
  const testDSiteDB = await setupNewDB()
  n = 0
  await testDSiteDB.single.query().eachUrl(result => {
    n++
    t.truthy(typeof result === 'string')
    // is .url
    t.truthy(result.startsWith('dweb://'))
    t.truthy(result.endsWith('.json'))
  })
  t.is(n, 10)
  n = 0
  await testDSiteDB.multi.query().eachUrl(result => {
    n++
    t.truthy(typeof result === 'string')
    // is .url
    t.truthy(result.startsWith('dweb://'))
    t.truthy(result.endsWith('.json'))
  })
  t.is(n, 30)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: urls()', async t => {
  var n
  var result
  const testDSiteDB = await setupNewDB()
  n = 0
  var urls = await testDSiteDB.single.query().urls()
  urls.forEach(result => {
    n++
    t.truthy(typeof result === 'string')
    // is .url
    t.truthy(result.startsWith('dweb://'))
    t.truthy(result.endsWith('.json'))
  })
  t.is(n, 10)
  n = 0
  urls = await testDSiteDB.multi.query().urls()
  urls.forEach(result => {
    n++
    t.truthy(typeof result === 'string')
    // is .url
    t.truthy(result.startsWith('dweb://'))
    t.truthy(result.endsWith('.json'))
  })
  t.is(n, 30)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: first()', async t => {
  var result
  const testDSiteDB = await setupNewDB()
  result = await testDSiteDB.single.query().first()
  t.truthy(result && 'first' in result && 'second' in result && 'third' in result)
  result = await testDSiteDB.multi.query().first()
  t.truthy(result && 'first' in result && 'second' in result && 'third' in result)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: last()', async t => {
  var result
  const testDSiteDB = await setupNewDB()
  result = await testDSiteDB.single.query().last()
  t.truthy(result && 'first' in result && 'second' in result && 'third' in result)
  result = await testDSiteDB.multi.query().last()
  t.truthy(result && 'first' in result && 'second' in result && 'third' in result)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: count()', async t => {
  var result
  const testDSiteDB = await setupNewDB()
  result = await testDSiteDB.single.query().count()
  t.is(result, 10)
  result = await testDSiteDB.multi.query().count()
  t.is(result, 30)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: orderBy()', async t => {
  var result
  const testDSiteDB = await setupNewDB()
  result = await testDSiteDB.single.query().orderBy('first').first()
  t.is(result.first, 'first0')
  result = await testDSiteDB.single.query().orderBy('second').first()
  t.is(result.second, 0)
  result = await testDSiteDB.single.query().orderBy('first+second').first()
  t.is(result.first, 'first0')
  t.is(result.second, 0)
  result = await testDSiteDB.single.query().orderBy('third').first()
  t.is(result.third, 'third0single')
  result = await testDSiteDB.multi.query().orderBy('first').first()
  t.is(result.first, 'first0')
  result = await testDSiteDB.multi.query().orderBy('second').first()
  t.is(result.second, 0)
  result = await testDSiteDB.multi.query().orderBy('first+second').first()
  t.is(result.first, 'first0')
  t.is(result.second, 0)
  result = await testDSiteDB.multi.query().orderBy('third').first()
  t.is(result.third, 'third0multi1')
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: reverse()', async t => {
  var result
  const testDSiteDB = await setupNewDB()
  result = await testDSiteDB.single.query().reverse().toArray()
  t.truthy(result[0].first, 'first9')
  result = await testDSiteDB.single.query().orderBy('second').reverse().toArray()
  t.truthy(result[0].first, 'first9')
  result = await testDSiteDB.multi.query().reverse().toArray()
  t.truthy(result[0].first, 'first9')
  result = await testDSiteDB.multi.query().orderBy('second').reverse().toArray()
  t.truthy(result[0].first, 'first9')
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: uniqueKeys()', async t => {
  var result
  const testDSiteDB = await setupNewDB()
  result = await testDSiteDB.single.query().uniqueKeys()
  t.is(result.length, 10)
  result = await testDSiteDB.multi.query().orderBy('first').uniqueKeys()
  t.is(result.length, 20)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: offset() and limit()', async t => {
  const testDSiteDB = await setupNewDB()
  var results = await testDSiteDB.single.query().orderBy('first').offset(1).toArray()
  t.is(results[0].first, 'first1')
  t.is(results.length, 9)
  var results = await testDSiteDB.single.query().orderBy('first').limit(2).toArray()
  t.is(results[0].first, 'first0')
  t.is(results[1].first, 'first1')
  t.is(results.length, 2)
  var results = await testDSiteDB.single.query().orderBy('first').offset(1).limit(2).toArray()
  t.is(results[0].first, 'first1')
  t.is(results[1].first, 'first2')
  t.is(results.length, 2)
  var results = await testDSiteDB.single.query().orderBy('first').offset(1).limit(2).reverse().toArray()
  t.is(results[0].first, 'first8')
  t.is(results[1].first, 'first7')
  t.is(results.length, 2)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: filter()', async t => {
  const testDSiteDB = await setupNewDB()
  var results = await testDSiteDB.single.query().filter(r => r.first === 'first5').toArray()
  t.is(results[0].first, 'first5')
  t.is(results.length, 1)
  var results = await testDSiteDB.single.query()
    .filter(r => r.first.startsWith('first'))
    .filter(r => r.second === 5)
    .toArray()
  t.is(results[0].first, 'first5')
  t.is(results.length, 1)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: until()', async t => {
  const testDSiteDB = await setupNewDB()
  var results = await testDSiteDB.single.orderBy('first').until(r => r.first === 'first5').toArray()
  t.is(results[0].first, 'first0')
  t.is(results[5].first, 'first5')
  t.is(results.length, 6)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: or()', async t => {
  // TODO
  t.pass()
})

dSiteDbTest('dSiteDB Tests: clone()', async t => {
  var result
  const testDSiteDB = await setupNewDB()
  var resultSet = testDSiteDB.single.orderBy('first')
  var resultSetClone = resultSet.clone().reverse()
  result = await resultSet.first()
  t.is(result.first, 'first0')
  result = await resultSetClone.first()
  t.is(result.first, 'first9')
  await testDSiteDB.close()
})
