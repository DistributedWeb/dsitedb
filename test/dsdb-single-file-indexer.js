const dSiteDbTest = require('ava')
const {newDSiteDB, reopenDB, ts} = require('./lib/util')
const DWebVault = require('@dpack/vault')
const tempy = require('tempy')
const Ajv = require('ajv')

dSiteDbTest.before(() => console.log('dsdb-single-file-indexer.js'))

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

dSiteDbTest('index and unindex single files', async t => {
  var testDSiteDB = await setupNewDB()

  // profile.json

  await testDSiteDB.indexFile(aliceVault, '/profile.json')

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
  await t.throws(testDSiteDB.profile.level.get(bobVault.url + '/profile.json'))
  await t.throws(testDSiteDB.broadcasts.level.get(aliceVault.url + '/broadcasts/' + aliceVault.broadcast1TS + '.json'))

  await testDSiteDB.unindexFile(aliceVault, '/profile.json')

  await t.throws(testDSiteDB.profile.level.get(aliceVault.url + '/profile.json'))
  await t.throws(testDSiteDB.profile.level.get(bobVault.url + '/profile.json'))
  await t.throws(testDSiteDB.broadcasts.level.get(aliceVault.url + '/broadcasts/' + aliceVault.broadcast1TS + '.json'))

  // 2 broadcasts

  await testDSiteDB.indexFile(aliceVault, '/broadcasts/' + aliceVault.broadcast1TS + '.json')
  await testDSiteDB.indexFile(aliceVault, '/broadcasts/' + aliceVault.broadcast2TS + '.json')

  await t.throws(testDSiteDB.profile.level.get(aliceVault.url + '/profile.json'))
  await t.throws(testDSiteDB.profile.level.get(bobVault.url + '/profile.json'))
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

  await testDSiteDB.unindexFile(aliceVault, '/broadcasts/' + aliceVault.broadcast1TS + '.json')
  await testDSiteDB.unindexFile(aliceVault, '/broadcasts/' + aliceVault.broadcast2TS + '.json')

  await t.throws(testDSiteDB.profile.level.get(aliceVault.url + '/profile.json'))
  await t.throws(testDSiteDB.profile.level.get(bobVault.url + '/profile.json'))
  await t.throws(testDSiteDB.broadcasts.level.get(aliceVault.url + '/broadcasts/' + aliceVault.broadcast1TS + '.json'))
  await t.throws(testDSiteDB.broadcasts.level.get(aliceVault.url + '/broadcasts/' + aliceVault.broadcast2TS + '.json'))

  await testDSiteDB.close()
})
