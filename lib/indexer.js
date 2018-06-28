
const flatten = require('lodash.flatten')
const anymatch = require('anymatch')
const LevelUtil = require('./util-level')
const {debug, veryDebug, lock, checkoutVault} = require('./util')

const READ_TIMEOUT = 30e3

// exported api
// =

exports.addVault = async function (db, vault, {watch}) {
  veryDebug('Indexer.addVault', vault.url, {watch})

  // store whether is writable
  var info = await vault.getInfo({timeout: READ_TIMEOUT})
  vault.isWritable = info.isOwner

  // process the vault
  await (
    indexVault(db, vault)
      .then(() => {
        if (watch) exports.watchVault(db, vault)
      })
      .catch(e => onFailInitialIndex(e, db, vault, {watch}))
  )
}

exports.removeVault = async function (db, vault) {
  veryDebug('Indexer.removeVault', vault.url)
  await unindexVault(db, vault)
  exports.unwatchVault(db, vault)
}

exports.watchVault = async function (db, vault) {
  veryDebug('Indexer.watchVault', vault.url)
  if (vault.fileEvents) {
    console.error('watchVault() called on vault that already is being watched', vault.url)
    return
  }
  if (vault._loadPromise) {
    // HACK dpack-vault fix
    // Because of a weird API difference btwn dpack-vault and beaker's DPackVault...
    // ...the event-stream methods need await _loadPromise
    // -prf
    await vault._loadPromise
  }
  vault.fileEvents = vault.createFileActivityStream(db._tableFilePatterns)
  // autodownload all changes to the watched files
  vault.fileEvents.addEventListener('invalidated', ({path}) => vault.download(path))
  // autoindex on changes
  // TODO debounce!!!!
  vault.fileEvents.addEventListener('changed', ({path}) => {
    indexVault(db, vault)
  })
}

exports.unwatchVault = function (db, vault) {
  veryDebug('Indexer.unwatchVault', vault.url)
  if (vault.fileEvents) {
    vault.fileEvents.close()
    vault.fileEvents = null
  }
}

exports.resetOutdatedIndexes = async function (db, neededRebuilds) {
  if (neededRebuilds.length === 0) {
    return false
  }
  debug(`Indexer.resetOutdatedIndexes need to rebuild ${neededRebuilds.length} tables`)
  veryDebug('Indexer.resetOutdatedIndexes tablesToRebuild', neededRebuilds)

  // clear tables
  // TODO go per-table
  const tables = db.tables
  for (let i = 0; i < tables.length; i++) {
    let table = tables[i]
    veryDebug('clearing', table.name)
    // clear indexed data
    await LevelUtil.clear(table.level)
  }

  // reset meta records
  var promises = []
  await LevelUtil.each(db._indexMetaLevel, indexMeta => {
    indexMeta.version = 0
    promises.push(db._indexMetaLevel.put(indexMeta.url, indexMeta))
  })
  await Promise.all(promises)

  return true
}

// figure how what changes need to be processed
// then update the indexes
async function indexVault (db, vault) {
  debug('Indexer.indexVault', vault.url)
  var release = await lock(`index:${vault.url}`)
  try {
    // sanity check
    if (!db.isOpen && !db.isBeingOpened) {
      return
    }
    if (!db.level) {
      return console.log('indexVault called on corrupted db')
    }

    // fetch the current state of the vault's index
    var [indexMeta, vaultMeta] = await Promise.all([
      db._indexMetaLevel.get(vault.url).catch(e => null),
      vault.getInfo({timeout: READ_TIMEOUT})
    ])
    indexMeta = indexMeta || {version: 0}
    try {
      db.emit('source-indexing', vault.url, indexMeta.version, vaultMeta.version)
    } catch (e) {
      console.error(e)
    }

    // has this version of the vault been processed?
    if (indexMeta && indexMeta.version >= vaultMeta.version) {
      debug('Indexer.indexVault no index needed for', vault.url)
      try {
        db.emit('source-indexed', vault.url, vaultMeta.version)
      } catch (e) {
        console.error(e)
      }
      return // yes, stop
    }
    debug('Indexer.indexVault', vault.url, 'start', indexMeta.version, 'end', vaultMeta.version)

    // find and apply all changes which haven't yet been processed
    var updates = await scanVaultHistoryForUpdates(db, vault, {
      start: indexMeta.version + 1,
      end: vaultMeta.version + 1
    })
    await applyUpdates(db, vault, updates)
    debug('Indexer.indexVault applied', updates.length, 'updates from', vault.url)

    // emit
    try {
      db.emit('source-indexed', vault.url, vaultMeta.version)
      db.emit('indexes-updated', vault.url, vaultMeta.version)
    } catch (e) {
      console.error(e)
    }
  } finally {
    release()
  }
}
exports.indexVault = indexVault

// delete all records generated from the vault
async function unindexVault (db, vault) {
  var release = await lock(`index:${vault.url}`)
  try {
    // find any relevant records and delete them from the indexes
    var recordMatches = await scanVaultForRecords(db, vault)
    await Promise.all(recordMatches.map(match => match.table.level.del(match.recordUrl)))
    await db._indexMetaLevel.del(vault.url)
  } finally {
    release()
  }
}
exports.unindexVault = unindexVault

// read the file, find the matching table, validate, then store
async function readAndIndexFile (db, vault, filepath, version = false) {
  const tables = db.tables
  const fileUrl = vault.url + filepath
  try {
    // read file
    var record = JSON.parse(await vault.readFile(filepath, {timeout: READ_TIMEOUT}))

    // index on the first matching table
    for (var i = 0; i < tables.length; i++) {
      let table = tables[i]
      if (table.isRecordFile(filepath)) {
        // validate
        let isValid = true
        if (table.schema.validate) {
          try { isValid = table.schema.validate(record) } catch (e) { isValid = false }
        }
        if (isValid) {
          // run preprocessor
          if (table.schema.preprocess) {
            let newRecord = table.schema.preprocess(record)
            if (newRecord) record = newRecord
          }
          // save
          let obj = {
            url: fileUrl,
            origin: vault.url,
            indexedAt: Date.now(),
            record
          }
          await table.level.put(fileUrl, obj)
          try { table.emit('put-record', obj) } catch (e) { console.error(e) }
        } else {
          // delete
          await table.level.del(fileUrl)
          try {
            table.emit('del-record', {
              url: fileUrl,
              origin: vault.url,
              indexedAt: Date.now()
            })
          } catch (e) { console.error(e) }
        }
      }
    }
  } catch (e) {
    console.log('Failed to index', fileUrl, e)
    throw e
  }
}
exports.readAndIndexFile = readAndIndexFile

async function unindexFile (db, vault, filepath) {
  const tables = db.tables
  const fileUrl = vault.url + filepath
  try {
    // unindex on the first matching table
    for (var i = 0; i < tables.length; i++) {
      let table = tables[i]
      if (table.isRecordFile(filepath)) {
        await table.level.del(fileUrl)
        try {
          table.emit('del-record', {
            url: fileUrl,
            origin: vault.url,
            indexedAt: Date.now()
          })
        } catch (e) { console.error(e) }
      }
    }
  } catch (e) {
    console.log('Failed to unindex', fileUrl, e)
  }
}
exports.unindexFile = unindexFile

// internal methods
// =

// helper for when the first indexVault() fails
// emit an error, and (if it's a timeout) keep looking for the vault
async function onFailInitialIndex (e, db, vault, {watch}) {
  if (e.name === 'TimeoutError') {
    debug('Indexer.onFailInitialIndex starting retry loop', vault.url)
    db.emit('source-missing', vault.url)
    while (true) {
      veryDebug('Indexer.onFailInitialIndex attempting load', vault.url)
      // try again every 30 seconds
      await new Promise(resolve => setTimeout(resolve, 30e3))
      // still a source?
      if (!db.isOpen || !(vault.url in db._vaults)) {
        return
      }
      // re-attempt the index
      try {
        await indexVault(db, vault)
        veryDebug('Indexer.onFailInitialIndex successfully loaded', vault.url)
        break // made it!
      } catch (e) {
        // abort if we get a non-timeout error
        if (e.name !== 'TimeoutError') {
          veryDebug('Indexer.onFailInitialIndex failed attempt, aborting', vault.url, e)
          return
        }
      }
    }
    // success
    db.emit('source-found', vault.url)
    if (watch) exports.watchVault(db, vault)
  } else {
    db.emit('source-error', vault.url, e)
  }
}

// look through the given history slice
// match against the tables' path patterns
// return back the *latest* change to each matching changed record, as an array ordered by revision
async function scanVaultHistoryForUpdates (db, vault, {start, end}) {
  var history = await vault.history({start, end, timeout: READ_TIMEOUT})

  // pull the latest update to each file
  var updates = {}
  history.forEach(update => {
    if (anymatch(db._tableFilePatterns, update.path)) {
      updates[update.path] = update
    }
  })

  // return an array ordered by version
  return Object.values(updates).sort((a, b) => a.version - b.version)
}

// look through the vault for any files that generate records
async function scanVaultForRecords (db, vault) {
  var recordFiles = await Promise.all(db.tables.map(table => {
    return table.listRecordFiles(vault)
  }))
  return flatten(recordFiles)
}

// iterate the updates and apply them one by one, updating the metadata as each is applied successfully
async function applyUpdates (db, vault, updates) {
  for (let i = 0; i < updates.length; i++) {
    // process update
    var update = updates[i]
    if (update.type === 'del') {
      await unindexFile(db, vault, update.path)
    } else {
      await readAndIndexFile(db, vault, update.path, update.version)
    }

    // update meta
    await LevelUtil.update(db._indexMetaLevel, vault.url, {
      url: vault.url,
      version: update.version // record the version we've indexed
    })
    try {
      db.emit('source-index-progress', vault.url, (i + 1), updates.length)
    } catch (e) {
      console.error(e)
    }
  }
}
