const dSiteDbTest = require('ava')
const {newDSiteDB, ts} = require('./lib/util')
const DWebVault = require('@dpack/vault')
const tempy = require('tempy')

dSiteDbTest.before(() => console.log('dsdb-where-clause.js'))

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
  async function def (i, fn) {
    const a = await DWebVault.create({localPath: tempy.directory(), author: {url: 'dweb://' + (i.toString().repeat(32))}})
    await a.mkdir('/multi')
    const write = (path, record) => a.writeFile(path, JSON.stringify(record))
    await fn(write, a)
    return a
  }
  for (let i = 0; i < 10; i++) {
    vaults.push(await def(i, async write => {
      await write('/single.json', {first: 'first' + i, second: i, third: 'third' + i + 'single'})
      await write('/multi/1.json', {first: 'first' + i, second: (i+1)*100, third: 'third' + i + 'multi1'})
      await write('/multi/2.json', {first: 'first' + i, second: i, third: 'third' + i + 'multi2'})
      await write('/multi/3.json', {first: 'first' + (i+1)*100, second: i, third: 'third' + i + 'multi3'})
    }))
  }
})

dSiteDbTest('dSiteDB Tests: above()', async t => {
  const testDSiteDB = await setupNewDB()
  var result = await testDSiteDB.single.where('first').above('first2').first()
  t.is(result.first, 'first3')
  var result = await testDSiteDB.single.where('second').above(3).first()
  t.is(result.second, 4)
  var result = await testDSiteDB.single.where('first+second').above(['first5',5]).first()
  t.is(result.first, 'first6')
  t.is(result.second, 6)
  var result = await testDSiteDB.single.where('first+second').above(['first5',0]).first()
  t.is(result.first, 'first5')
  t.is(result.second, 5)
  var result = await testDSiteDB.single.where('first+second').above(['first5',6]).first()
  t.is(result.first, 'first6')
  t.is(result.second, 6)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: aboveOrEqual()', async t => {
  const testDSiteDB = await setupNewDB()
  var result = await testDSiteDB.single.where('first').aboveOrEqual('first2').first()
  t.is(result.first, 'first2')
  var result = await testDSiteDB.single.where('second').aboveOrEqual(3).first()
  t.is(result.second, 3)
  var result = await testDSiteDB.single.where('first+second').aboveOrEqual(['first5',5]).first()
  t.is(result.first, 'first5')
  t.is(result.second, 5)
  var result = await testDSiteDB.single.where('first+second').aboveOrEqual(['first5',0]).first()
  t.is(result.first, 'first5')
  t.is(result.second, 5)
  var result = await testDSiteDB.single.where('first+second').aboveOrEqual(['first5',6]).first()
  t.is(result.first, 'first6')
  t.is(result.second, 6)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: anyOf()', async t => {
  const testDSiteDB = await setupNewDB()
  var results = await testDSiteDB.single.where('first').anyOf('first2', 'first4', 'first6').toArray()
  t.is(results.length, 3)
  t.is(results[0].first, 'first2')
  t.is(results[1].first, 'first4')
  t.is(results[2].first, 'first6')
  var results = await testDSiteDB.single.where('second').anyOf(2, 4, 6).toArray()
  t.is(results.length, 3)
  t.is(results[0].second, 2)
  t.is(results[1].second, 4)
  t.is(results[2].second, 6)
  var results = await testDSiteDB.single.where('first').anyOf('first0', 'first10000').toArray()
  t.is(results.length, 1)
  t.is(results[0].first, 'first0')
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: anyOfIgnoreCase()', async t => {
  const testDSiteDB = await setupNewDB()
  var results = await testDSiteDB.single.where('first').anyOfIgnoreCase('FIRST2', 'FIRST4', 'FIRST6').toArray()
  t.is(results.length, 3)
  t.is(results[0].first, 'first2')
  t.is(results[1].first, 'first4')
  t.is(results[2].first, 'first6')
  var results = await testDSiteDB.single.where('second').anyOfIgnoreCase(2, 4, 6).toArray()
  t.is(results.length, 3)
  t.is(results[0].second, 2)
  t.is(results[1].second, 4)
  t.is(results[2].second, 6)
  var results = await testDSiteDB.single.where('first').anyOfIgnoreCase('FIRST0', 'FIRST10000').toArray()
  t.is(results.length, 1)
  t.is(results[0].first, 'first0')
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: below()', async t => {
  const testDSiteDB = await setupNewDB()
  var result = await testDSiteDB.single.where('first').below('first2').last()
  t.is(result.first, 'first1')
  var result = await testDSiteDB.single.where('second').below(3).last()
  t.is(result.second, 2)
  var result = await testDSiteDB.single.where('first+second').below(['first5',5]).last()
  t.is(result.first, 'first4')
  t.is(result.second, 4)
  var result = await testDSiteDB.single.where('first+second').below(['first5',0]).last()
  t.is(result.first, 'first4')
  t.is(result.second, 4)
  var result = await testDSiteDB.single.where('first+second').below(['first5',6]).last()
  t.is(result.first, 'first5')
  t.is(result.second, 5)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: belowOrEqual()', async t => {
  const testDSiteDB = await setupNewDB()
  var result = await testDSiteDB.single.where('first').belowOrEqual('first2').last()
  t.is(result.first, 'first2')
  var result = await testDSiteDB.single.where('second').belowOrEqual(3).last()
  t.is(result.second, 3)
  var result = await testDSiteDB.single.where('first+second').belowOrEqual(['first5',5]).last()
  t.is(result.first, 'first5')
  t.is(result.second, 5)
  var result = await testDSiteDB.single.where('first+second').belowOrEqual(['first5',0]).last()
  t.is(result.first, 'first4')
  t.is(result.second, 4)
  var result = await testDSiteDB.single.where('first+second').belowOrEqual(['first5',6]).last()
  t.is(result.first, 'first5')
  t.is(result.second, 5)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: between()', async t => {
  const testDSiteDB = await setupNewDB()
  var results = await testDSiteDB.single.where('first').between('first2', 'first4').toArray()
  t.is(results.length, 1)
  t.is(results[0].first, 'first3')
  var results = await testDSiteDB.single.where('first').between('first2', 'first4', {includeLower: true}).toArray()
  t.is(results.length, 2)
  t.is(results[0].first, 'first2')
  t.is(results[1].first, 'first3')
  var results = await testDSiteDB.single.where('first').between('first2', 'first4', {includeUpper: true}).toArray()
  t.is(results.length, 2)
  t.is(results[0].first, 'first3')
  t.is(results[1].first, 'first4')
  var results = await testDSiteDB.single.where('first').between('first2', 'first4', {includeLower: true, includeUpper: true}).toArray()
  t.is(results.length, 3)
  t.is(results[0].first, 'first2')
  t.is(results[1].first, 'first3')
  t.is(results[2].first, 'first4')
  var results = await testDSiteDB.single.where('second').between(2, 4).toArray()
  t.is(results.length, 1)
  t.is(results[0].second, 3)
  var results = await testDSiteDB.single.where('second').between(2, 4, {includeLower: true}).toArray()
  t.is(results.length, 2)
  t.is(results[0].second, 2)
  t.is(results[1].second, 3)
  var results = await testDSiteDB.single.where('second').between(2, 4, {includeUpper: true}).toArray()
  t.is(results.length, 2)
  t.is(results[0].second, 3)
  t.is(results[1].second, 4)
  var results = await testDSiteDB.single.where('second').between(2, 4, {includeLower: true, includeUpper: true}).toArray()
  t.is(results.length, 3)
  t.is(results[0].second, 2)
  t.is(results[1].second, 3)
  t.is(results[2].second, 4)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: equals()', async t => {
  const testDSiteDB = await setupNewDB()
  var result = await testDSiteDB.single.where('first').equals('first2').first()
  t.is(result.first, 'first2')
  var result = await testDSiteDB.single.where('second').equals(3).first()
  t.is(result.second, 3)
  var result = await testDSiteDB.single.where('first+second').equals(['first4',4]).first()
  t.is(result.first, 'first4')
  t.is(result.second, 4)
  var result = await testDSiteDB.single.where('first').equals('no match').first()
  t.falsy(result)
  var result = await testDSiteDB.single.where(':origin').equals(vaults[0].url).first()
  t.is(result.first, 'first0')
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: equalsIgnoreCase()', async t => {
  const testDSiteDB = await setupNewDB()
  var result = await testDSiteDB.single.where('first').equalsIgnoreCase('FIRST2').first()
  t.is(result.first, 'first2')
  var result = await testDSiteDB.single.where('second').equalsIgnoreCase(3).first()
  t.is(result.second, 3)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: noneOf()', async t => {
  const testDSiteDB = await setupNewDB()
  var results = await testDSiteDB.single.where('first').noneOf('first2', 'first4', 'first6').toArray()
  t.is(results.length, 7)
  var results = await testDSiteDB.single.where('second').noneOf(2, 4, 6).toArray()
  t.is(results.length, 7)
  var results = await testDSiteDB.single.where('first').noneOf('first0', 'first10000').toArray()
  t.is(results.length, 9)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: notEqual()', async t => {
  const testDSiteDB = await setupNewDB()
  var results = await testDSiteDB.single.where('first').notEqual('first2').toArray()
  t.is(results.length, 9)
  var results = await testDSiteDB.single.where('second').notEqual(2).toArray()
  t.is(results.length, 9)
  var results = await testDSiteDB.single.where('first').noneOf('first10000').toArray()
  t.is(results.length, 10)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: startsWith()', async t => {
  const testDSiteDB = await setupNewDB()
  var results = await testDSiteDB.single.where('first').startsWith('first').toArray()
  t.is(results.length, 10)
  var results = await testDSiteDB.single.where('first').startsWith('nomatch').toArray()
  t.is(results.length, 0)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: startsWithAnyOf()', async t => {
  const testDSiteDB = await setupNewDB()
  var results = await testDSiteDB.single.where('first').startsWithAnyOf('first1', 'first2').toArray()
  t.is(results.length, 2)
  var results = await testDSiteDB.single.where('first').startsWithAnyOf('first', 'nomatch').toArray()
  t.is(results.length, 10)
  var results = await testDSiteDB.single.where('first').startsWithAnyOf('nomatch').toArray()
  t.is(results.length, 0)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: startsWithAnyOfIgnoreCase()', async t => {
  const testDSiteDB = await setupNewDB()
  var results = await testDSiteDB.single.where('first').startsWithAnyOfIgnoreCase('FIRST1', 'FIRST2').toArray()
  t.is(results.length, 2)
  var results = await testDSiteDB.single.where('first').startsWithAnyOfIgnoreCase('FIRST', 'NOMATCH').toArray()
  t.is(results.length, 10)
  var results = await testDSiteDB.single.where('first').startsWithAnyOfIgnoreCase('NOMATCH').toArray()
  t.is(results.length, 0)
  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: startsWithIgnoreCase()', async t => {
  const testDSiteDB = await setupNewDB()
  var results = await testDSiteDB.single.where('first').startsWithIgnoreCase('FIRST').toArray()
  t.is(results.length, 10)
  var results = await testDSiteDB.single.where('first').startsWithIgnoreCase('NOMATCH').toArray()
  t.is(results.length, 0)
  await testDSiteDB.close()
})
