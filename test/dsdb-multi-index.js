const dSiteDbTest = require('ava')
const {newDSiteDB, ts} = require('./lib/util')
const DPackVault = require('@dpack/vault')
const tempy = require('tempy')

dSiteDbTest.before(() => console.log('dsdb-multi-index.js'))

var vaults = []

async function setupNewDB () {
  const testDSiteDB = newDSiteDB()
  testDSiteDB.define('table', {
    filePattern: '/table/*.json',
    index: [
      'key',
      {name: 'sports', def: ['*sports', '*theSports']},
      {name: 'city', def: ['city', 'colour', 'borough']},
      {name: 'cityCompound', def: ['city+key', 'cities+key', 'borough+key']}
    ]
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
    await write('/table/1.json', {key: 1, sports: 'basketball', city: 'dallas'})
    await write('/table/2.json', {key: 2, theSports: ['basketball', 'football'], colour: 'minnesota'})
    await write('/table/3.json', {key: 3, sports: ['basketball', 'football', 'hockey'], borough: 'miami'})
  }))
  vaults.push(await def(async write => {
    await write('/table/1.json', {key: 1, theSports: 'hockey', city: 'minnesota'})
    await write('/table/2.json', {key: 2, sports: ['basketball', 'football'], colour: 'dallas'})
  }))
})

function normalizeRecord (record) {
  if (record.theSports) record.sports = record.theSports
  if (record.colour) record.city = record.colour
  if (record.borough) record.city = record.borough
  return record
}

dSiteDbTest('dSiteDB Tests: above()', async t => {
  const testDSiteDB = await setupNewDB()

  var results = await testDSiteDB.table.where('sports').above('football').toArray()
  t.is(results.length, 2)
  t.is(results.length, results.map(normalizeRecord).filter(v => v.sports === 'hockey' || v.sports.indexOf('hockey') >= 0).length)

  results = await testDSiteDB.table.where('city').above('minnesota').toArray()
  t.is(results.length, 2)
  t.is(results.length, results.map(normalizeRecord).filter(v => v.city === 'dallas').length)

  results = await testDSiteDB.table.where('cityCompound').above(['minnesota', 1]).toArray()
  t.is(results.length, 3)
  t.is(results.length, results.map(normalizeRecord).filter(v => v.city === 'dallas' || v.key === 2).length)

  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: aboveOrEqual()', async t => {
  const testDSiteDB = await setupNewDB()

  var results = await testDSiteDB.table.where('sports').aboveOrEqual('football').toArray()
  t.truthy(results.length > 0)
  t.is(results.map(normalizeRecord).filter(v => v.sports === 'basketball').length, 0)

  results = await testDSiteDB.table.where('city').aboveOrEqual('minnesota').toArray()
  t.is(results.length, 4)
  t.is(results.length, results.map(normalizeRecord).filter(v => v.city === 'minnesota' || v.city === 'dallas').length)

  results = await testDSiteDB.table.where('cityCompound').aboveOrEqual(['minnesota', 1]).toArray()
  t.is(results.length, 4)
  t.is(results.length, results.map(normalizeRecord).filter(v => v.city === 'dallas' || v.city === 'minnesota').length)

  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: anyOf()', async t => {
  const testDSiteDB = await setupNewDB()

  var results = await testDSiteDB.table.where('sports').anyOf('football', 'hockey').toArray()
  t.truthy(results.length > 0)
  t.is(results.map(normalizeRecord).filter(v => v.sports === 'basketball').length, 0)

  results = await testDSiteDB.table.where('city').anyOf('minnesota', 'dallas').toArray()
  t.is(results.length, 4)
  t.is(results.length, results.map(normalizeRecord).filter(v => v.city === 'minnesota' || v.city === 'dallas').length)

  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: anyOfIgnoreCase()', async t => {
  const testDSiteDB = await setupNewDB()

  var results = await testDSiteDB.table.where('sports').anyOfIgnoreCase('FOOTBA', 'HOCKY').toArray()
  t.truthy(results.length > 0)
  t.is(results.map(normalizeRecord).filter(v => v.sports === 'basketball').length, 0)

  results = await testDSiteDB.table.where('city').anyOfIgnoreCase('MINNESOTA', 'DALLAS').toArray()
  t.is(results.length, 4)
  t.is(results.length, results.map(normalizeRecord).filter(v => v.city === 'minnesota' || v.city === 'dallas').length)

  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: below()', async t => {
  const testDSiteDB = await setupNewDB()

  var results = await testDSiteDB.table.where('sports').below('football').toArray()
  t.is(results.length, 4)
  t.is(results.length, results.map(normalizeRecord).filter(v => v.sports === 'basketball' || v.sports.indexOf('basketball') >= 0).length)

  results = await testDSiteDB.table.where('city').below('minnesota').toArray()
  t.is(results.length, 1)
  t.is(results.length, results.map(normalizeRecord).filter(v => v.city === 'miami').length)

  results = await testDSiteDB.table.where('cityCompound').below(['minnesota', 2]).toArray()
  t.is(results.length, 2)
  t.is(results.length, results.map(normalizeRecord).filter(v => v.city === 'miami' || v.key === 1).length)

  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: belowOrEqual()', async t => {
  const testDSiteDB = await setupNewDB()

  var results = await testDSiteDB.table.where('sports').belowOrEqual('football').toArray()
  t.truthy(results.length > 0)
  t.is(results.length, results.map(normalizeRecord).filter(v => v.sports === 'basketball' || v.sports.indexOf('basketball') >= 0).length)

  results = await testDSiteDB.table.where('city').belowOrEqual('minnesota').toArray()
  t.is(results.length, 3)
  t.is(results.length, results.map(normalizeRecord).filter(v => v.city === 'miami' || v.city === 'minnesota').length)

  results = await testDSiteDB.table.where('cityCompound').belowOrEqual(['minnesota', 2]).toArray()
  t.is(results.length, 3)
  t.is(results.length, results.map(normalizeRecord).filter(v => v.city === 'miami' || v.city === 'minnesota').length)

  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: between()', async t => {
  const testDSiteDB = await setupNewDB()

  var results = await testDSiteDB.table.where('sports').between('basketball', 'hockey').toArray()
  t.truthy(results.length > 0)
  t.is(results.length, results.map(normalizeRecord).filter(v => v.sports === 'football' || v.sports.indexOf('football') >= 0).length)

  results = await testDSiteDB.table.where('city').between('charlotte', 'sacramento').toArray()
  t.is(results.length, 2)
  t.is(results.length, results.map(normalizeRecord).filter(v => v.city === 'minnesota').length)

  results = await testDSiteDB.table.where('cityCompound').between(['minnesota', 1], ['minnesota', 3]).toArray()
  t.is(results.length, 1)
  t.is(results.length, results.map(normalizeRecord).filter(v => v.city === 'minnesota' && v.key === 2).length)

  results = await testDSiteDB.table.where('cityCompound').between(['minnesota', 1], ['minnesota', 3], {includeLower: true}).toArray()
  t.is(results.length, 2)
  t.is(results.length, results.map(normalizeRecord).filter(v => v.city === 'minnesota').length)

  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: equals()', async t => {
  const testDSiteDB = await setupNewDB()

  var results = await testDSiteDB.table.where('sports').equals('football').toArray()
  t.truthy(results.length > 0)
  t.is(results.length, results.map(normalizeRecord).filter(v => v.sports === 'football' || v.sports.indexOf('football') >= 0).length)

  results = await testDSiteDB.table.where('city').equals('minnesota').toArray()
  t.is(results.length, 2)
  t.is(results.length, results.map(normalizeRecord).filter(v => v.city === 'minnesota').length)

  results = await testDSiteDB.table.where('cityCompound').equals(['minnesota', 2]).toArray()
  t.is(results.length, 1)
  t.is(results.length, results.map(normalizeRecord).filter(v => v.city === 'minnesota' && v.key === 2).length)

  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: equalsIgnoreCase()', async t => {
  const testDSiteDB = await setupNewDB()

  var results = await testDSiteDB.table.where('sports').equalsIgnoreCase('FOOTBA').toArray()
  t.truthy(results.length > 0)
  t.is(results.length, results.map(normalizeRecord).filter(v => v.sports === 'football' || v.sports.indexOf('football') >= 0).length)

  results = await testDSiteDB.table.where('city').equalsIgnoreCase('MINNESOTA').toArray()
  t.is(results.length, 2)
  t.is(results.length, results.map(normalizeRecord).filter(v => v.city === 'minnesota').length)

  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: noneOf()', async t => {
  const testDSiteDB = await setupNewDB()

  var results = await testDSiteDB.table.where('sports').noneOf('basketball', 'hockey').toArray()
  t.truthy(results.length > 0)
  t.is(results.length, results.map(normalizeRecord).filter(v => v.sports === 'football' || v.sports.indexOf('football') >= 0).length)

  results = await testDSiteDB.table.where('city').noneOf('dallas', 'miami').toArray()
  t.is(results.length, 2)
  t.is(results.length, results.map(normalizeRecord).filter(v => v.city === 'minnesota').length)

  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: notEqual()', async t => {
  const testDSiteDB = await setupNewDB()

  var results = await testDSiteDB.table.where('sports').notEqual('hockey').toArray()
  t.truthy(results.length > 0)
  t.is(0, results.map(normalizeRecord).filter(v => v.sports === 'hockey').length)

  results = await testDSiteDB.table.where('city').notEqual('dallas').toArray()
  t.is(results.length, 3)
  t.is(results.length, results.map(normalizeRecord).filter(v => v.city === 'minnesota' || v.city === 'miami').length)

  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: startsWith()', async t => {
  const testDSiteDB = await setupNewDB()

  var results = await testDSiteDB.table.where('sports').startsWith('footb').toArray()
  t.truthy(results.length > 0)
  t.is(results.length, results.map(normalizeRecord).filter(v => v.sports === 'football' || v.sports.indexOf('football') >= 0).length)

  results = await testDSiteDB.table.where('city').startsWith('m').toArray()
  t.is(results.length, 2)
  t.is(results.length, results.map(normalizeRecord).filter(v => v.city === 'minnesota').length)

  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: startsWithAnyOf()', async t => {
  const testDSiteDB = await setupNewDB()

  var results = await testDSiteDB.table.where('sports').startsWithAnyOf('footb', 'hock').toArray()
  t.truthy(results.length > 0)
  t.is(results.map(normalizeRecord).filter(v => v.sports === 'basketball').length, 0)

  results = await testDSiteDB.table.where('city').startsWithAnyOf('m', 'm').toArray()
  t.is(results.length, 3)
  t.is(results.length, results.map(normalizeRecord).filter(v => v.city === 'miami' || v.city === 'minnesota').length)

  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: startsWithAnyOfIgnoreCase()', async t => {
  const testDSiteDB = await setupNewDB()

  var results = await testDSiteDB.table.where('sports').startsWithAnyOfIgnoreCase('FOOTB', 'HOCK').toArray()
  t.truthy(results.length > 0)
  t.is(results.map(normalizeRecord).filter(v => v.sports === 'basketball').length, 0)

  results = await testDSiteDB.table.where('city').startsWithAnyOfIgnoreCase('M', 'M').toArray()
  t.is(results.length, 3)
  t.is(results.length, results.map(normalizeRecord).filter(v => v.city === 'miami' || v.city === 'minnesota').length)

  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: startsWithIgnoreCase()', async t => {
  const testDSiteDB = await setupNewDB()

  var results = await testDSiteDB.table.where('sports').startsWithIgnoreCase('FOOTB').toArray()
  t.truthy(results.length > 0)
  t.is(results.length, results.map(normalizeRecord).filter(v => v.sports === 'football' || v.sports.indexOf('football') >= 0).length)

  results = await testDSiteDB.table.where('city').startsWithIgnoreCase('M').toArray()
  t.is(results.length, 2)
  t.is(results.length, results.map(normalizeRecord).filter(v => v.city === 'minnesota').length)

  await testDSiteDB.close()
})
