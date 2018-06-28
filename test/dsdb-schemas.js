const dSiteDbTest = require('ava')
const {newDSiteDB, reopenDB} = require('./lib/util')

dSiteDbTest.before(() => console.log('dsdb-schemas.js'))

dSiteDbTest('dSiteDB Tests: one table', async t => {
  const testDSiteDB = newDSiteDB()

  // setup the schema
  testDSiteDB.define('firstTable', {
    path: '/table1/*.json',
    buildPath: record => `/table1/${record.id}.json`,
    index: ['a', 'b', 'c']
  })
  var res = await testDSiteDB.open()

  // check that the table was created correctly
  t.deepEqual(res, {rebuilds: ['firstTable']})
  t.truthy(testDSiteDB.firstTable)
  t.truthy(testDSiteDB.firstTable.level)
  t.deepEqual(Object.keys(testDSiteDB.firstTable.level.indexes), ['a', 'b', 'c', ':origin'])

  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: two tables', async t => {
  const testDSiteDB = newDSiteDB()

  // setup the schema
  testDSiteDB.define('firstTable', {
    path: '/table1/*.json',
    buildPath: record => `/table1/${record.id}.json`,
    index: ['a', 'b', 'c']
  })
  testDSiteDB.define('secondTable', {
    path: '/table2/*.json',
    buildPath: record => `/table2/${record.id}.json`,
    index: ['d', 'e', 'f']
  })
  var res = await testDSiteDB.open()

  // check that the table was created correctly
  t.deepEqual(res, {rebuilds: ['firstTable', 'secondTable']})
  t.truthy(testDSiteDB.firstTable)
  t.truthy(testDSiteDB.firstTable.level)
  t.deepEqual(Object.keys(testDSiteDB.firstTable.level.indexes), ['a', 'b', 'c', ':origin'])
  t.truthy(testDSiteDB.secondTable)
  t.truthy(testDSiteDB.secondTable.level)
  t.deepEqual(Object.keys(testDSiteDB.secondTable.level.indexes), ['d', 'e', 'f', ':origin'])

  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: properly detect changes', async t => {
  const testDSiteDB = newDSiteDB()

  // setup the schema
  testDSiteDB.define('firstTable', {
    path: '/table1/*.json',
    buildPath: record => `/table1/${record.id}.json`,
    index: ['a', 'b', 'c']
  })
  testDSiteDB.define('secondTable', {
    path: '/table2/*.json',
    buildPath: record => `/table2/${record.id}.json`,
    index: ['d', 'e', 'f']
  })
  var res = await testDSiteDB.open()

  // check that the table was created correctly
  t.truthy(testDSiteDB.firstTable)
  t.truthy(testDSiteDB.firstTable.level)
  t.deepEqual(Object.keys(testDSiteDB.firstTable.level.indexes), ['a', 'b', 'c', ':origin'])
  t.truthy(testDSiteDB.secondTable)
  t.truthy(testDSiteDB.secondTable.level)
  t.deepEqual(Object.keys(testDSiteDB.secondTable.level.indexes), ['d', 'e', 'f', ':origin'])

  await testDSiteDB.close()
  const testDSiteDB2 = reopenDB(testDSiteDB)

  // change the firstTable schema
  testDSiteDB2.define('firstTable', {
    path: '/table1/*.json',
    buildPath: record => `/table1/${record.id}.json`,
    index: ['g', 'h', 'i']
  })
  testDSiteDB2.define('secondTable', {
    path: '/table2/*.json',
    buildPath: record => `/table2/${record.id}.json`,
    index: ['d', 'e', 'f']
  })
  res = await testDSiteDB2.open()

  // check that the table was created correctly
  t.deepEqual(res, {rebuilds: ['firstTable']})
  t.truthy(testDSiteDB2.firstTable)
  t.truthy(testDSiteDB2.firstTable.level)
  t.deepEqual(Object.keys(testDSiteDB2.firstTable.level.indexes), ['g', 'h', 'i', ':origin'])
  t.truthy(testDSiteDB2.secondTable)
  t.truthy(testDSiteDB2.secondTable.level)
  t.deepEqual(Object.keys(testDSiteDB2.secondTable.level.indexes), ['d', 'e', 'f', ':origin'])

  await testDSiteDB2.close()
  const testDSiteDB3 = reopenDB(testDSiteDB)

  // change the both schemas
  testDSiteDB3.define('firstTable', {
    path: '/table1/*.json',
    buildPath: record => `/table1/${record.id}.json`,
    index: ['g', 'h', 'i', 'j']
  })
  testDSiteDB3.define('secondTable', {
    path: '/table2/*.json',
    buildPath: record => `/table2/${record.id}.json`,
    index: ['d', 'e', 'f', 'g']
  })
  res = await testDSiteDB3.open()

  // check that the table was created correctly
  t.deepEqual(res, {rebuilds: ['firstTable', 'secondTable']})
  t.truthy(testDSiteDB3.firstTable)
  t.truthy(testDSiteDB3.firstTable.level)
  t.deepEqual(Object.keys(testDSiteDB3.firstTable.level.indexes), ['g', 'h', 'i', 'j', ':origin'])
  t.truthy(testDSiteDB3.secondTable)
  t.truthy(testDSiteDB3.secondTable.level)
  t.deepEqual(Object.keys(testDSiteDB3.secondTable.level.indexes), ['d', 'e', 'f', 'g', ':origin'])

  await testDSiteDB3.close()
})

dSiteDbTest('dSiteDB Tests: complex index dSiteDbTest', async t => {
  const testDSiteDB = newDSiteDB()

  // setup the schema
  testDSiteDB.define('firstTable', {
    path: '/table1/*.json',
    buildPath: record => `/table1/${record.id}.json`,
    index: ['a+b', 'c+d', 'e']
  })
  await testDSiteDB.open()

  // check that the table was created correctly
  t.truthy(testDSiteDB.firstTable)
  t.truthy(testDSiteDB.firstTable.level)
  t.deepEqual(Object.keys(testDSiteDB.firstTable.level.indexes), ['a+b', 'c+d', 'e', ':origin'])

  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: multi-def index dSiteDbTest', async t => {
  const testDSiteDB = newDSiteDB()

  // setup the schema
  testDSiteDB.define('firstTable', {
    path: '/table1/*.json',
    buildPath: record => `/table1/${record.id}.json`,
    index: [
      {name: 'a', def: ['a', 'b']},
      {name: 'c+d', def: ['c+d', 'cee+dee']},
      {name: 'e', def: ['*e', '*eee']}
    ]
  })
  await testDSiteDB.open()

  // check that the table was created correctly
  t.truthy(testDSiteDB.firstTable)
  t.truthy(testDSiteDB.firstTable.level)
  t.deepEqual(Object.keys(testDSiteDB.firstTable.level.indexes), ['a', 'c+d', 'e', ':origin'])

  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: multi-def index must have matching definitions', async t => {
  const testDSiteDB = newDSiteDB()

  t.throws(() => testDSiteDB.define('firstTable', {
    path: '/table1/*.json',
    buildPath: record => `/table1/${record.id}.json`,
    index: [
      {name: 'a', def: ['a', 'b+c']}
    ]
  }))

  t.throws(() => testDSiteDB.define('firstTable', {
    path: '/table1/*.json',
    buildPath: record => `/table1/${record.id}.json`,
    index: [
      {name: 'a', def: ['*a', 'b']}
    ]
  }))

  await testDSiteDB.close()
})
