const dSiteDbTest = require('ava')
const level = require('memdb')
const getStream = require('get-stream')
const IndexedLevel = require('../lib/indexed-level.js')

dSiteDbTest.before(() => console.log('dsdb-indexed-level.js'))

dSiteDbTest('dSiteDB Tests: indexes', async t => {
  const db = IndexedLevel(level({ valueEncoding: 'json'}), [
    {name: 'lastName', def: 'lastName'},
    {name: 'fullName', def: 'lastName+firstName'},
    {name: 'attributes', def: '*attributes'},
  ])

  const JARED = {record: {firstName: 'Jared', lastName: 'Rice', attributes: ['entrepreneur', 'coder']}}
  const STAN = {record: {firstName: 'Stan', lastName: 'Rice', attributes: ['entrepreneur', 'coder']}}
  const MIKE = {record: {firstName: 'Mike', lastName: 'Taggart', attributes: ['entrepreneur', 'coder']}}

  await db.put(1, JARED)
  await db.put(2, STAN)
  await db.put(3, MIKE)

  // dSiteDbTest all getters

  t.deepEqual(await db.get(1), JARED)
  t.deepEqual(await db.get(2), STAN)
  t.deepEqual(await db.get(3), MIKE)
  t.deepEqual(await db.indexes.lastName.get('Rice'), JARED)
  t.deepEqual(await db.indexes.lastName.get('Taggart'), MIKE)

  t.deepEqual(await db.indexes['fullName'].get(['Rice', 'Jared']), JARED)
  t.deepEqual(await db.indexes['fullName'].get(['Rice', 'Stan']), STAN)
  t.deepEqual(await db.indexes['fullName'].get(['Taggart', 'Mike']), MIKE)

  t.deepEqual(await db.indexes.attributes.get('entrepreneur'), JARED)
  t.deepEqual(await db.indexes.attributes.get('entrepreneur'), JARED)
  t.deepEqual(await db.indexes.attributes.get('entrepreneur'), MIKE)

  // dSiteDbTest normal stream behavior

  t.deepEqual(await getStream.array(db.createReadStream()), [{key: '1', value: JARED}, {key: '2', value: STAN}, {key: '3', value: MIKE}])
  t.deepEqual(await getStream.array(db.createReadStream({gte: 2})), [{key: '2', value: STAN}, {key: '3', value: MIKE}])
  t.deepEqual(await getStream.array(db.createReadStream({gte: 2, lt: 3})), [{key: '2', value: STAN}])

  t.deepEqual(await getStream.array(db.createKeyStream()), ['1', '2', '3'])
  t.deepEqual(await getStream.array(db.createKeyStream({gte: 2})), ['2', '3'])
  t.deepEqual(await getStream.array(db.createKeyStream({gte: 2, lt: 3})), ['2'])

  t.deepEqual(await getStream.array(db.createValueStream()), [JARED, STAN, MIKE])
  t.deepEqual(await getStream.array(db.createValueStream({gte: 2})), [STAN, MIKE])
  t.deepEqual(await getStream.array(db.createValueStream({gte: 2, lt: 3})), [STAN])

  // dSiteDbTest index stream behavior

  t.deepEqual(await getStream.array(db.indexes.lastName.createReadStream()), [{key: 1, value: JARED}, {key: 2, value: STAN}, {key: 3, value: MIKE}])
  t.deepEqual(await getStream.array(db.indexes.lastName.createReadStream({gt: 'Rice'})), [{key: 3, value: MIKE}])

  t.deepEqual(await getStream.array(db.indexes.lastName.createKeyStream()), [1, 2, 3])
  t.deepEqual(await getStream.array(db.indexes.lastName.createKeyStream({gt: 'Rice'})), [3])

  t.deepEqual(await getStream.array(db.indexes.lastName.createValueStream()), [JARED, STAN, MIKE])
  t.deepEqual(await getStream.array(db.indexes.lastName.createValueStream({gt: 'Rice'})), [MIKE])

  // dSiteDbTest index ranges

  t.deepEqual(await getStream.array(db.indexes.lastName.createValueStream({gte: 'Rice'})), [JARED, STAN, MIKE])
  t.deepEqual(await getStream.array(db.indexes.lastName.createValueStream({lte: 'Rice'})), [JARED, STAN])
  t.deepEqual(await getStream.array(db.indexes.lastName.createValueStream({gte: 'Rice', lte: 'Taggart'})), [JARED, STAN, MIKE])
  t.deepEqual(await getStream.array(db.indexes.lastName.createValueStream({gt: 'Rice', lte: 'Taggart'})), [MIKE])
  t.deepEqual(await getStream.array(db.indexes.lastName.createValueStream({gte: 'Rice', lt: 'Taggart'})), [JARED, STAN])

  // dSiteDbTest compound index ranges

  t.deepEqual(await getStream.array(db.indexes['fullName'].createValueStream({gte: ['Rice']})), [STAN, JARED, MIKE])
  t.deepEqual(await getStream.array(db.indexes['fullName'].createValueStream({lte: ['Rice']})), [])
  t.deepEqual(await getStream.array(db.indexes['fullName'].createValueStream({lte: ['Rice', 'Stan']})), [STAN])
  t.deepEqual(await getStream.array(db.indexes['fullName'].createValueStream({lt: ['Rice', 'Stan']})), [])
  t.deepEqual(await getStream.array(db.indexes['fullName'].createValueStream({gte: ['Rice'], lte: ['Taggart']})), [STAN, JARED])
  t.deepEqual(await getStream.array(db.indexes['fullName'].createValueStream({gt: ['Rice'], lte: ['Taggart']})), [STAN, JARED])
  t.deepEqual(await getStream.array(db.indexes['fullName'].createValueStream({gt: ['Rice'], lte: ['Taggart', 'Mike']})), [STAN, JARED, MIKE])
  t.deepEqual(await getStream.array(db.indexes['fullName'].createValueStream({gte: ['Rice'], lt: ['Taggart']})), [STAN, JARED])

  // dSiteDbTest multiple index ranges

  t.deepEqual(await getStream.array(db.indexes.attributes.createValueStream({gte: 'entrepreneur', lte: 'coder'})), [JARED, MIKE])
  t.deepEqual(await getStream.array(db.indexes.attributes.createValueStream({gte: 'entrepreneur', lte: 'coder'})), [JARED, STAN])
  t.deepEqual(await getStream.array(db.indexes.attributes.createValueStream({gte: 'entrepreneur', lte: 'coder'})), [MIKE])

  // dSiteDbTest modifications

  STAN.record.attributes.push('entrepreneur')
  STAN.record.attributes.push('weirdo')
  STAN.record.lastName = 'Ford'
  await db.put(2, STAN)

  // dSiteDbTest all getters

  t.deepEqual(await db.indexes.lastName.get('Rice'), JARED)
  t.deepEqual(await db.indexes.lastName.get('Ford'), STAN)
  t.deepEqual(await db.indexes.lastName.get('Taggart'), MIKE)

  t.deepEqual(await db.indexes['fullName'].get(['Rice', 'Jared']), JARED)
  t.deepEqual(await db.indexes['fullName'].get(['Ford', 'Stan']), STAN)
  t.deepEqual(await db.indexes['fullName'].get(['Taggart', 'Mike']), MIKE)

  t.deepEqual(await db.indexes.attributes.get('entrepreneur'), JARED)
  t.deepEqual(await db.indexes.attributes.get('entrepreneur'), JARED)
  t.deepEqual(await db.indexes.attributes.get('entrepreneur'), MIKE)
  t.deepEqual(await db.indexes.attributes.get('weirdo'), STAN)

  // dSiteDbTest normal stream behavior

  t.deepEqual(await getStream.array(db.createReadStream()), [{key: '1', value: JARED}, {key: '2', value: STAN}, {key: '3', value: MIKE}])
  t.deepEqual(await getStream.array(db.createReadStream({gte: 2})), [{key: '2', value: STAN}, {key: '3', value: MIKE}])
  t.deepEqual(await getStream.array(db.createReadStream({gte: 2, lt: 3})), [{key: '2', value: STAN}])

  t.deepEqual(await getStream.array(db.createKeyStream()), ['1', '2', '3'])
  t.deepEqual(await getStream.array(db.createKeyStream({gte: 2})), ['2', '3'])
  t.deepEqual(await getStream.array(db.createKeyStream({gte: 2, lt: 3})), ['2'])

  t.deepEqual(await getStream.array(db.createValueStream()), [JARED, STAN, MIKE])
  t.deepEqual(await getStream.array(db.createValueStream({gte: 2})), [STAN, MIKE])
  t.deepEqual(await getStream.array(db.createValueStream({gte: 2, lt: 3})), [STAN])

  // dSiteDbTest index stream behavior

  t.deepEqual(await getStream.array(db.indexes.lastName.createReadStream()), [{key: 1, value: JARED}, {key: 2, value: STAN}, {key: 3, value: MIKE}])
  t.deepEqual(await getStream.array(db.indexes.lastName.createReadStream({gt: 'Rice'})), [{key: 2, value: STAN}, {key: 3, value: MIKE}])

  t.deepEqual(await getStream.array(db.indexes.lastName.createKeyStream()), [1, 2, 3])
  t.deepEqual(await getStream.array(db.indexes.lastName.createKeyStream({gt: 'Rice'})), [2, 3])

  t.deepEqual(await getStream.array(db.indexes.lastName.createValueStream()), [JARED, STAN, MIKE])
  t.deepEqual(await getStream.array(db.indexes.lastName.createValueStream({gt: 'Rice'})), [STAN, MIKE])

  // dSiteDbTest index ranges

  t.deepEqual(await getStream.array(db.indexes.lastName.createValueStream({gte: 'Rice'})), [JARED, STAN, MIKE])
  t.deepEqual(await getStream.array(db.indexes.lastName.createValueStream({lte: 'Rice'})), [JARED])
  t.deepEqual(await getStream.array(db.indexes.lastName.createValueStream({gte: 'Rice', lte: 'Taggart'})), [JARED, STAN, MIKE])
  t.deepEqual(await getStream.array(db.indexes.lastName.createValueStream({gt: 'Rice', lte: 'Taggart'})), [STAN, MIKE])
  t.deepEqual(await getStream.array(db.indexes.lastName.createValueStream({gte: 'Rice', lt: 'Taggart'})), [JARED, STAN])

  // dSiteDbTest compound index ranges

  t.deepEqual(await getStream.array(db.indexes['fullName'].createValueStream({gte: ['Rice']})), [JARED, STAN, MIKE])
  t.deepEqual(await getStream.array(db.indexes['fullName'].createValueStream({lte: ['Rice']})), [])
  t.deepEqual(await getStream.array(db.indexes['fullName'].createValueStream({lte: ['Ford', 'Stan']})), [JARED, STAN])
  t.deepEqual(await getStream.array(db.indexes['fullName'].createValueStream({lt: ['Ford', 'Stan']})), [JARED])
  t.deepEqual(await getStream.array(db.indexes['fullName'].createValueStream({gte: ['Rice'], lte: ['Taggart']})), [JARED, STAN])
  t.deepEqual(await getStream.array(db.indexes['fullName'].createValueStream({gt: ['Rice'], lte: ['Taggart']})), [JARED, STAN])
  t.deepEqual(await getStream.array(db.indexes['fullName'].createValueStream({gt: ['Rice'], lte: ['Taggart', 'Mike']})), [JARED, STAN, MIKE])
  t.deepEqual(await getStream.array(db.indexes['fullName'].createValueStream({gte: ['Rice'], lt: ['Taggart']})), [JARED, STAN])

  // dSiteDbTest multiple index ranges

  t.deepEqual(await getStream.array(db.indexes.attributes.createValueStream({gte: 'entrepreneur', lte: 'entrepreneur'})), [JARED, MIKE, STAN])
  t.deepEqual(await getStream.array(db.indexes.attributes.createValueStream({gte: 'entrepreneur', lte: 'entrepreneur'})), [JARED, STAN])
  t.deepEqual(await getStream.array(db.indexes.attributes.createValueStream({gte: 'entrepreneur', lte: 'entrepreneur'})), [MIKE])

  // dSiteDbTest deletions

  await db.del(2)

  // dSiteDbTest all getters

  t.deepEqual(await db.indexes.lastName.get('Rice'), JARED)
  t.deepEqual(await db.indexes.lastName.get('Taggart'), MIKE)

  t.deepEqual(await db.indexes['fullName'].get(['Rice', 'Jared']), JARED)
  t.deepEqual(await db.indexes['fullName'].get(['Taggart', 'Mike']), MIKE)

  t.deepEqual(await db.indexes.attributes.get('entrepreneur'), JARED)
  t.deepEqual(await db.indexes.attributes.get('entrepreneur'), JARED)
  t.deepEqual(await db.indexes.attributes.get('entrepreneur'), MIKE)

  // dSiteDbTest normal stream behavior

  t.deepEqual(await getStream.array(db.createReadStream()), [{key: '1', value: JARED}, {key: '3', value: MIKE}])
  t.deepEqual(await getStream.array(db.createReadStream({gte: 2})), [{key: '3', value: MIKE}])
  t.deepEqual(await getStream.array(db.createReadStream({gte: 2, lt: 3})), [])

  t.deepEqual(await getStream.array(db.createKeyStream()), ['1', '3'])
  t.deepEqual(await getStream.array(db.createKeyStream({gte: 2})), ['3'])
  t.deepEqual(await getStream.array(db.createKeyStream({gte: 2, lt: 3})), [])

  t.deepEqual(await getStream.array(db.createValueStream()), [JARED, MIKE])
  t.deepEqual(await getStream.array(db.createValueStream({gte: 2})), [MIKE])
  t.deepEqual(await getStream.array(db.createValueStream({gte: 2, lt: 3})), [])

  // dSiteDbTest index stream behavior

  t.deepEqual(await getStream.array(db.indexes.lastName.createReadStream()), [{key: 1, value: JARED}, {key: 3, value: MIKE}])
  t.deepEqual(await getStream.array(db.indexes.lastName.createReadStream({gt: 'Rice'})), [{key: 3, value: MIKE}])

  t.deepEqual(await getStream.array(db.indexes.lastName.createKeyStream()), [1, 3])
  t.deepEqual(await getStream.array(db.indexes.lastName.createKeyStream({gt: 'Rice'})), [3])

  t.deepEqual(await getStream.array(db.indexes.lastName.createValueStream()), [JARED, MIKE])
  t.deepEqual(await getStream.array(db.indexes.lastName.createValueStream({gt: 'Rice'})), [MIKE])

  // dSiteDbTest index ranges

  t.deepEqual(await getStream.array(db.indexes.lastName.createValueStream({gte: 'Rice'})), [JARED, MIKE])
  t.deepEqual(await getStream.array(db.indexes.lastName.createValueStream({lte: 'Rice'})), [JARED])
  t.deepEqual(await getStream.array(db.indexes.lastName.createValueStream({gte: 'Rice', lte: 'Taggart'})), [JARED, MIKE])
  t.deepEqual(await getStream.array(db.indexes.lastName.createValueStream({gt: 'Rice', lte: 'Taggart'})), [MIKE])
  t.deepEqual(await getStream.array(db.indexes.lastName.createValueStream({gte: 'Rice', lt: 'Taggart'})), [JARED])

  // dSiteDbTest compound index ranges

  t.deepEqual(await getStream.array(db.indexes['fullName'].createValueStream({gte: ['Rice']})), [JARED, MIKE])
  t.deepEqual(await getStream.array(db.indexes['fullName'].createValueStream({lte: ['Rice']})), [])
  t.deepEqual(await getStream.array(db.indexes['fullName'].createValueStream({lte: ['Ford', 'Stan']})), [JARED])
  t.deepEqual(await getStream.array(db.indexes['fullName'].createValueStream({lt: ['Ford', 'Stan']})), [JARED])
  t.deepEqual(await getStream.array(db.indexes['fullName'].createValueStream({gte: ['Rice'], lte: ['Taggart']})), [JARED])
  t.deepEqual(await getStream.array(db.indexes['fullName'].createValueStream({gt: ['Rice'], lte: ['Taggart']})), [JARED])
  t.deepEqual(await getStream.array(db.indexes['fullName'].createValueStream({gt: ['Rice'], lte: ['Taggart', 'Mike']})), [JARED, MIKE])
  t.deepEqual(await getStream.array(db.indexes['fullName'].createValueStream({gte: ['Rice'], lt: ['Taggart']})), [JARED])

  // dSiteDbTest multiple index ranges

  t.deepEqual(await getStream.array(db.indexes.attributes.createValueStream({gte: 'entrepreneur', lte: 'entrepreneur'})), [JARED, MIKE])
  t.deepEqual(await getStream.array(db.indexes.attributes.createValueStream({gte: 'entrepreneur', lte: 'entrepreneur'})), [JARED])
  t.deepEqual(await getStream.array(db.indexes.attributes.createValueStream({gte: 'entrepreneur', lte: 'entrepreneur'})), [MIKE])
})
