const dSiteDbTest = require('ava')
const {newDSiteDB} = require('./lib/util')
const tempy = require('tempy')

dSiteDbTest.before(() => console.log('dsdb-helper-tables.js'))

var vaults = []

async function setupNewDB () {
  const testDSiteDB = newDSiteDB()
  testDSiteDB.define('helper', {
    helperTable: true,
    index: ['color', 'height']
  })
  await testDSiteDB.open()
  return testDSiteDB
}

dSiteDbTest('dSiteDB Tests: put(), get(), and delete()', async t => {
  const testDSiteDB = await setupNewDB()

  await testDSiteDB.helper.put('thing1', {color: 'blue', height: 5, width: 2})
  await testDSiteDB.helper.put('thing2', {color: 'red', height: 6, width: 1})
  await testDSiteDB.helper.put('thing3', {color: 'blue', height: 2, width: 2})

  t.deepEqual(getObjData(await testDSiteDB.helper.get('thing1')), {color: 'blue', height: 5, width: 2})
  t.deepEqual(getObjData(await testDSiteDB.helper.get('thing2')), {color: 'red', height: 6, width: 1})
  t.deepEqual(getObjData(await testDSiteDB.helper.get('thing3')), {color: 'blue', height: 2, width: 2})

  t.deepEqual(getObjData(await testDSiteDB.helper.get('color', 'red')), {color: 'red', height: 6, width: 1})
  t.deepEqual(getObjData(await testDSiteDB.helper.get('height', 2)), {color: 'blue', height: 2, width: 2})

  await testDSiteDB.helper.delete('thing3')
  t.deepEqual(await testDSiteDB.helper.get('thing3'), undefined)
  t.deepEqual(await testDSiteDB.helper.get('height', 2), undefined)
})

dSiteDbTest('dSiteDB Tests: queries', async t => {
  const testDSiteDB = await setupNewDB()

  await testDSiteDB.helper.put('thing1', {color: 'blue', height: 5, width: 2})
  await testDSiteDB.helper.put('thing2', {color: 'red', height: 6, width: 1})
  await testDSiteDB.helper.put('thing3', {color: 'blue', height: 2, width: 2})

  t.deepEqual(getObjData(await testDSiteDB.helper.where('color').equals('blue').first()), {color: 'blue', height: 5, width: 2})
  t.deepEqual(getObjData((await testDSiteDB.helper.where('color').equalsIgnoreCase('RED').toArray())[0]), {color: 'red', height: 6, width: 1})
  t.deepEqual(getObjData(await testDSiteDB.helper.orderBy('color').offset(1).first()), {color: 'blue', height: 2, width: 2})

  await testDSiteDB.helper.where('color').equals('blue').update({color: 'BLUE'})
  await testDSiteDB.helper.orderBy('height').update(record => { record.height = record.height + 1; return record })

  t.deepEqual(getObjData(await testDSiteDB.helper.get('thing1')), {color: 'BLUE', height: 6, width: 2})
  t.deepEqual(getObjData(await testDSiteDB.helper.get('thing2')), {color: 'red', height: 7, width: 1})
  t.deepEqual(getObjData(await testDSiteDB.helper.get('thing3')), {color: 'BLUE', height: 3, width: 2})
})

function getObjData (obj) {
  for (var k in obj) {
    if (typeof obj[k] === 'function') {
      delete obj[k]
    }
  }
  return obj
}
