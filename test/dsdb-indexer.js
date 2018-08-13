const dSiteDbTest = require('ava')
const path = require('path')
const fs = require('fs')
const {newDSiteDB, reopenDB, ts} = require('./lib/util')
const DWebVault = require('@dpack/vault')
const tempy = require('tempy')
const Ajv = require('ajv')

dSiteDbTest.before(() => console.log('dsdb-indexer.js'))

var aliceVault
var bobVault

async function setupNewDB () {
  const testDSiteDB = newDSiteDB()
  testDSiteDB.define('profile', {
    filePattern: '/profile.json',
    index: 'name',
    validate: (new Ajv()).compile({
      type: 'object',
      properties: {
        name: {type: 'string'},
        bio: {type: 'string'}
      },
      required: ['name']
    })
  })
  testDSiteDB.define('broadcasts', {
    filePattern: '/broadcasts/*.json',
    index: ['createdAt', 'type+createdAt'],
    validate: (new Ajv()).compile({
      type: 'object',
      properties: {
        type: {type: 'string'},
        createdAt: {type: 'number'}
      },
      required: ['type', 'createdAt']
    })
  })
  await testDSiteDB.open()
  return testDSiteDB
}

dSiteDbTest.before('dSiteDB Tests: setup vaults', async () => {
  // setup alice
  const a = aliceVault = await DWebVault.create({
    localPath: tempy.directory(),
    title: 'Alice Vault',
    author: {url: 'dweb://ffffffffffffffffffffffffffffffff'}
  })
  await a.writeFile('/profile.json', JSON.stringify({name: 'alice', bio: 'Cool computer girl', avatarUrl: 'alice.png'}))
  await a.mkdir('/broadcasts')
  a.broadcast1TS = ts()
  await a.writeFile(`/broadcasts/${a.broadcast1TS}.json`, JSON.stringify({type: 'comment', text: 'Greetings, martian!', createdAt: a.broadcast1TS}))
  a.broadcast2TS = ts()
  await a.writeFile(`/broadcasts/${a.broadcast2TS}.json`, JSON.stringify({type: 'comment', text: 'Whoop', createdAt: a.broadcast2TS}))
  a.broadcast3TS = ts()
  await a.writeFile(`/broadcasts/${a.broadcast3TS}.json`, JSON.stringify({type: 'image', imageUrl: 'foo.png', createdAt: a.broadcast3TS}))
  await a.writeFile(`/broadcasts/bad.json`, JSON.stringify({this: 'is not included'}))

  // setup bob
  const b = bobVault = await DWebVault.create({
    localPath: tempy.directory(),
    title: 'Bob Vault'
  })
  await b.writeFile('/profile.json', JSON.stringify({name: 'bob', bio: 'Cool computer guy', avatarUrl: 'alice.png'}))
  await b.mkdir('/broadcasts')
  b.broadcast1TS = ts()
  await b.writeFile(`/broadcasts/${b.broadcast1TS}.json`, JSON.stringify({type: 'comment', text: 'Greetings, martian!', createdAt: b.broadcast1TS}))
  b.broadcast2TS = ts()
  await b.writeFile(`/broadcasts/${b.broadcast2TS}.json`, JSON.stringify({type: 'image', imageUrl: 'baz.png', createdAt: b.broadcast2TS}))
  await b.writeFile(`/broadcasts/bad.json`, JSON.stringify({this: 'is not included'}))
})

dSiteDbTest('dSiteDB Tests: index an vault', async t => {
  t.plan(27)

  // index the vault
  var testDSiteDB = await setupNewDB()

  // dSiteDbTest the source-indexing event
  testDSiteDB.on('source-indexing', (url, startVersion, targetVersion) => {
    t.is(url, aliceVault.url)
    t.is(startVersion, 0)
    t.is(targetVersion, 7)
  })

  testDSiteDB.on('source-index-progress', (url, tick, total) => {
    t.truthy(tick <= 5)
    t.is(total, 5)
    t.is(url, aliceVault.url)
  })

  // dSiteDbTest the put event
  testDSiteDB.profile.on('put-record', ({url, origin, record}) => {
    t.deepEqual(url, `${aliceVault.url}/profile.json`)
    t.deepEqual(origin, aliceVault.url)
    t.deepEqual(record, {
      name: 'alice',
      bio: 'Cool computer girl',
      avatarUrl: 'alice.png'
    })
  })

  // dSiteDbTest the source-indexed event
  testDSiteDB.on('source-indexed', (url, targetVersion) => {
    t.is(url, aliceVault.url)
    t.is(targetVersion, 7)
  })

  await testDSiteDB.indexVault(aliceVault)

  // dSiteDbTest the indexed values
  await testAliceIndex(t, testDSiteDB)

  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: handle indexing failures', async t => {
  t.plan(8)

  // index the vault
  var testDSiteDB = await setupNewDB()

  // dSiteDbTest the put event
  testDSiteDB.profile.on('put-record', ({url, origin, record}) => {
    t.deepEqual(url, `${aliceVault.url}/profile.json`)
    t.deepEqual(origin, aliceVault.url)
    t.deepEqual(record, {
      name: 'alice',
      bio: 'Cool computer girl',
      avatarUrl: 'alice.png'
    })
  })

  // setup reads to fail
  let readFile = aliceVault.readFile
  aliceVault.readFile = () => { throw new Error('Failed to read') }

  // try indexing (should fail)
  await testDSiteDB.indexVault(aliceVault)

  // no data
  await t.throws(testDSiteDB.profile.level.get(aliceVault.url + '/profile.json'))

  // restore reads
  aliceVault.readFile = readFile

  // // try indexing (should succeed)
  await testDSiteDB.indexVault(aliceVault)

  // // dSiteDbTest the indexed values
  await testAliceIndex(t, testDSiteDB)

  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: index two vaults', async t => {
  // index the vault
  var testDSiteDB = await setupNewDB()
  await Promise.all([
    testDSiteDB.indexVault(aliceVault),
    testDSiteDB.indexVault(bobVault)
  ])

  // dSiteDbTest the indexed values
  await testAliceIndex(t, testDSiteDB)
  await testBobIndex(t, testDSiteDB)

  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: index, delete db, then reindex', async t => {
  var testDSiteDB = await setupNewDB()

  // index the vault
  await testDSiteDB.indexVault(aliceVault)
  await testAliceIndex(t, testDSiteDB)

  // delete db
  await testDSiteDB.delete()
  await testDSiteDB.open()

  // index the vault
  await testDSiteDB.indexVault(aliceVault)
  await testAliceIndex(t, testDSiteDB)

  await testDSiteDB.close()
})

dSiteDbTest('dSiteDB Tests: make schema changes that require a full rebuild', async t => {
  // index the vault
  var testDSiteDB = await setupNewDB()
  await Promise.all([
    testDSiteDB.indexVault(aliceVault),
    testDSiteDB.indexVault(bobVault)
  ])

  // dSiteDbTest the indexed values
  await testAliceIndex(t, testDSiteDB)
  await testBobIndex(t, testDSiteDB)

  // grab counts
  var profileCount = await testDSiteDB.profile.count()
  var broadcastsCount = await testDSiteDB.broadcasts.count()
  t.is(profileCount, 2)
  t.truthy(broadcastsCount > 0)

  // close, make destructive change, and reopen
  await testDSiteDB.close()
  const testDSiteDB2 = reopenDB(testDSiteDB)
  testDSiteDB2.define('profile', {
    filePattern: '/profile.json',
    index: ['name', 'bio'],
    validate: (new Ajv()).compile({
      type: 'object',
      properties: {
        name: {type: 'string'},
        bio: {type: 'string'}
      },
      required: ['name']
    })
  })
  testDSiteDB2.define('broadcasts', {
    filePattern: '/broadcasts/*.json',
    index: ['createdAt', 'type', 'type+createdAt'],
    validate: (new Ajv()).compile({
      type: 'object',
      properties: {
        type: {type: 'string'},
        createdAt: {type: 'number'}
      },
      required: ['type', 'createdAt']
    })
  })
  var res = await testDSiteDB2.open()
  t.deepEqual(res, {rebuilds: ['profile', 'broadcasts']})
  await Promise.all([
    testDSiteDB2.indexVault(aliceVault),
    testDSiteDB2.indexVault(bobVault)
  ])

    // dSiteDbTest the indexed values
  // await testAliceIndex(t, testDSiteDB2)
  // await testBobIndex(t, testDSiteDB2)

  // check counts
  t.is(profileCount, await testDSiteDB2.profile.count())
  t.is(broadcastsCount, await testDSiteDB2.broadcasts.count())
  await testDSiteDB2.close()
})

dSiteDbTest('dSiteDB Tests: index two vaults, then make changes', async t => {
  // index the vault
  var testDSiteDB = await setupNewDB()
  await Promise.all([
    testDSiteDB.indexVault(aliceVault),
    testDSiteDB.indexVault(bobVault)
  ])

  // dSiteDbTest the indexed values
  await testAliceIndex(t, testDSiteDB)
  await testBobIndex(t, testDSiteDB)

  testDSiteDB.on('source-indexing', (url, startVersion, targetVersion) => {
    t.is(url, aliceVault.url)
    t.is(typeof startVersion, 'number')
    t.is(typeof targetVersion, 'number')
    t.truthy(startVersion <= targetVersion)
  })
  testDSiteDB.on('source-index-progress', (url, tick, total) => {
    t.is(tick, 1)
    t.is(total, 1)
    t.is(url, aliceVault.url)
  })
  testDSiteDB.on('source-indexed', (url, targetVersion) => {
    t.is(url, aliceVault.url)
    t.is(typeof targetVersion, 'number')
  })

  // write changes to alice's profile.json
  await aliceVault.writeFile('/profile.json', JSON.stringify({name: 'alice', bio: '_Very_ cool computer girl'}))
  await new Promise(resolve => testDSiteDB.once('indexes-updated', resolve))

  // dSiteDbTest updated values
  var profile = await testDSiteDB.profile.level.get(aliceVault.url + '/profile.json')
  t.truthy(profile)
  t.is(profile.record.name, 'alice')
  t.is(profile.record.bio, '_Very_ cool computer girl')

  // add a new broadcast to alice
  aliceVault.broadcast4TS = ts()
  await aliceVault.writeFile(`/broadcasts/${aliceVault.broadcast4TS}.json`, JSON.stringify({type: 'comment', text: 'Index me!', createdAt: aliceVault.broadcast4TS}))
  await new Promise(resolve => testDSiteDB.once('indexes-updated', resolve))

  // dSiteDbTest updated values
  var broadcast4 = await testDSiteDB.broadcasts.level.get(aliceVault.url + `/broadcasts/${aliceVault.broadcast4TS}.json`)
  t.truthy(broadcast4)
  t.is(broadcast4.record.type, 'comment')
  t.is(broadcast4.record.text, 'Index me!')
  t.is(broadcast4.record.createdAt, aliceVault.broadcast4TS)

  // delete broadcast 1 from alice
  await aliceVault.unlink(`/broadcasts/${aliceVault.broadcast1TS}.json`)
  await new Promise(resolve => testDSiteDB.once('indexes-updated', resolve))

  // dSiteDbTest updated values
  try {
    var broadcast4 = await testDSiteDB.broadcasts.level.get(aliceVault.url + `/broadcasts/${aliceVault.broadcast1TS}.json`)
    t.fail('should not hit')
  } catch (e) {
    t.truthy(e)
  }

  await testDSiteDB.close()
})

async function testAliceIndex (t, testDSiteDB) {
  var profile = await testDSiteDB.profile.level.get(aliceVault.url + '/profile.json')
  t.deepEqual(profile, {
    url: aliceVault.url + '/profile.json',
    origin: aliceVault.url,
    indexedAt: profile.indexedAt,
    record: {
      avatarUrl: 'alice.png',
      name: 'alice',
      bio: 'Cool computer girl'
    }
  })
  var broadcast1 = await testDSiteDB.broadcasts.level.get(aliceVault.url + '/broadcasts/' + aliceVault.broadcast1TS + '.json')
  t.deepEqual(broadcast1, {
    url: aliceVault.url + '/broadcasts/' + aliceVault.broadcast1TS + '.json',
    origin: aliceVault.url,
    indexedAt: broadcast1.indexedAt,
    record: {
      type: 'comment',
      text: 'Greetings, martian!',
      createdAt: aliceVault.broadcast1TS
    }
  })
  var broadcast2 = await testDSiteDB.broadcasts.level.get(aliceVault.url + '/broadcasts/' + aliceVault.broadcast2TS + '.json')
  t.deepEqual(broadcast2, {
    url: aliceVault.url + '/broadcasts/' + aliceVault.broadcast2TS + '.json',
    origin: aliceVault.url,
    indexedAt: broadcast2.indexedAt,
    record: {
      type: 'comment',
      text: 'Whoop',
      createdAt: aliceVault.broadcast2TS
    }
  })
  var broadcast3 = await testDSiteDB.broadcasts.level.get(aliceVault.url + '/broadcasts/' + aliceVault.broadcast3TS + '.json')
  t.deepEqual(broadcast3, {
    url: aliceVault.url + '/broadcasts/' + aliceVault.broadcast3TS + '.json',
    origin: aliceVault.url,
    indexedAt: broadcast3.indexedAt,
    record: {
      type: 'image',
      imageUrl: 'foo.png',
      createdAt: aliceVault.broadcast3TS
    }
  })
}

async function testBobIndex (t, testDSiteDB) {
  var profile = await testDSiteDB.profile.level.get(bobVault.url + '/profile.json')
  t.deepEqual(profile, {
    url: bobVault.url + '/profile.json',
    origin: bobVault.url,
    indexedAt: profile.indexedAt,
    record: {
      avatarUrl: 'alice.png',
      name: 'bob',
      bio: 'Cool computer guy'
    }
  })
  var broadcast1 = await testDSiteDB.broadcasts.level.get(bobVault.url + '/broadcasts/' + bobVault.broadcast1TS + '.json')
  t.deepEqual(broadcast1, {
    url: bobVault.url + '/broadcasts/' + bobVault.broadcast1TS + '.json',
    origin: bobVault.url,
    indexedAt: broadcast1.indexedAt,
    record: {
      type: 'comment',
      text: 'Greetings, martian!',
      createdAt: bobVault.broadcast1TS
    }
  })
  var broadcast2 = await testDSiteDB.broadcasts.level.get(bobVault.url + '/broadcasts/' + bobVault.broadcast2TS + '.json')
  t.deepEqual(broadcast2, {
    url: bobVault.url + '/broadcasts/' + bobVault.broadcast2TS + '.json',
    origin: bobVault.url,
    indexedAt: broadcast2.indexedAt,
    record: {
      type: 'image',
      imageUrl: 'baz.png',
      createdAt: bobVault.broadcast2TS
    }
  })
}
