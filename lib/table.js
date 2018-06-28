const anymatch = require('anymatch')
const EventEmitter = require('events')
const sublevel = require('subleveldown')
const IndexedLevel = require('./indexed-level')
const Indexer = require('./indexer')
const DSiteDBQuery = require('./query')
const {assert, debug, veryDebug, lock, toVaultUrl, URL} = require('./util')
const {ParameterError, QueryError} = require('./errors')

// exported api
// =

class DSiteDBTable extends EventEmitter {
  constructor (db, name, schema) {
    super()
    this.db = db
    this.name = name
    this.schema = schema
    this.isHelperTable = !!schema.helperTable
    veryDebug('DSiteDBTable', this.name, this.schema)
    this._filePattern = schema.filePattern || '*.json'

    // construct db object
    this.level = IndexedLevel(sublevel(db.level, name, {valueEncoding: 'json'}), schema.index)
  }

  // queries
  // =

  // () => DSiteDBQuery
  query () {
    return new DSiteDBQuery(this)
  }

  // () => Promise<Number>
  async count () {
    return this.query().count()
  }

  // (url) => Promise<url>
  async delete (url) {
    return this.where(':url').equals(url).delete()
  }

  // (Function) => Promise<Void>
  async each (fn) {
    return this.query().each(fn)
  }

  // (Function) => DSiteDBQuery
  filter (fn) {
    return this.query().filter(fn)
  }

  // (url) => Promise<Object>
  // (key, value) => Promise<Object>
  async get (...args) {
    if (args.length === 2) {
      return getByKeyValue(this, ...args)
    }
    return getByRecordUrl(this, ...args)
  }

  // (Number) => DSiteDBQuery
  limit (n) {
    return this.query().limit(n)
  }

  // (Number) => DSiteDBQuery
  offset (n) {
    return this.query().offset(n)
  }

  // (index) => DSiteDBQuery
  orderBy (index) {
    return this.query().orderBy(index)
  }

  // (url, record) => Promise<url>
  async put (url, record, noLockNeeded = false) {
    assert(url && typeof url === 'string', ParameterError, 'The first parameter of .put() must be url')
    assert(record && typeof record === 'object', ParameterError, 'The second parameter of .put() must be a record object')

    // run validation
    if (this.schema.validate) {
      let isValid = this.schema.validate(record)
      if (!isValid) throw new Error('The record did not pass validation.')
    }

    // run preprocessor
    if (this.schema.preprocess) {
      let newRecord = this.schema.preprocess(record)
      if (newRecord) record = newRecord
    }

    // run serializer
    if (this.schema.serialize) {
      let newRecord = this.schema.serialize(record)
      if (newRecord) record = newRecord
    }

    if (this.isHelperTable) {
      // write the KV
      await this.level.put(url, {
        url,
        origin: '',
        indexedAt: Date.now(),
        record
      })
    } else {
      // lookup the vault
      var urlp = new URL(url)
      var origin = urlp.protocol + '//' + urlp.hostname
      var vault = this.db._vaults[origin]
      if (!vault) {
        throw new QueryError('Unable to put(): the given vault is not part of the index')
      }
      if (!vault.isWritable) {
        throw new QueryError('Unable to put(): the given vault is not owned by this user')
      }

      // write the file
      debug('Table.put', urlp.pathname)
      veryDebug('Table.put vault', vault.url)
      veryDebug('Table.put record', record)
      var release = noLockNeeded === true ? noop : await lock(toVaultUrl(vault))
      try {
        await vault.writeFile(urlp.pathname, JSON.stringify(record))
        if (typeof vault.commit === 'function') {
          // legacy api
          await vault.commit()
        }
        await Indexer.indexVault(this.db, vault)
        return url
      } finally {
        release()
      }
    }
  }

  // () => DSiteDBQuery
  reverse () {
    return this.query().reverse()
  }

  // () => Promise<Array>
  async toArray () {
    return this.query().toArray()
  }

  // (url, Object|Function) => Promise<Number>
  async update (url, objOrFn) {
    return updateByUrl(this, url, objOrFn)
  }

  // (url, Object|Function) => Promise<url>
  async upsert (url, objOrFn) {
    assert(url && typeof url === 'string', ParameterError, 'The first parameter of .upsert() must be a url')
    assert(objOrFn && (typeof objOrFn === 'object' || typeof objOrFn === 'function'), ParameterError, 'The second parameter of .upsert() must be a record object or an update function')

    // update or add
    var release = await lock(url)
    try {
      var changes = await updateByUrl(this, url, objOrFn, true)
      if (changes === 0) {
        return this.put(url, typeof objOrFn === 'function' ? objOrFn() : objOrFn, true)
      }
      return changes
    } finally {
      release()
    }
  }

  // (index|query) => DSiteDBWhereClause|DSiteDBQuery
  where (indexOrQuery) {
    return this.query().where(indexOrQuery)
  }

  // record helpers
  // =

  // (String) => Boolean
  isRecordFile (filepath) {
    if (this.isHelperTable) {
      return false
    }
    return anymatch(this._filePattern, filepath)
  }

  // (DPackVault) => Array<Object>
  async listRecordFiles (vault) {
    if (this.isHelperTable) {
      return []
    }
    try {
      // scan for matching records
      let records = await vault.readdir('/', {recursive: true})
      return records.filter(name => anymatch(this._filePattern, name)).map(name => {
        return {
          recordUrl: vault.url + '/' + this.name + '/' + name,
          table: this
        }
      })
    } catch (e) {
      return []
    }
  }
}

function getByKeyValue (table, key, value) {
  debug('getByKeyValue')
  veryDebug('getByKeyValue table', table.name)
  veryDebug('getByKeyValue key', key)
  veryDebug('getByKeyValue value', value)
  return table.where(key).equals(value).first()
}

function getByRecordUrl (table, url) {
  debug('getByRecordUrl')
  veryDebug('getByRecordUrl table', table.name)
  veryDebug('getByRecordUrl url', url)
  return table.where(':url').equals(url).first()
}

async function updateByUrl (table, url, updates, noLockNeeded = false) {
  debug('updateByUrl')
  url = url && url.url ? url.url : url
  veryDebug('updateByUrl table', table.name)
  veryDebug('updateByUrl url', url)
  veryDebug('updateByUrl updates', updates)
  assert(typeof url === 'string', ParameterError, 'Invalid parameters given to update()')
  assert(updates && (typeof updates === 'object' || typeof updates === 'function'), ParameterError, 'Invalid parameters given to update()')

  var release = noLockNeeded === true ? noop : await lock(toVaultUrl(url))
  try {
    return table.where(':url').equals(url).update(updates)
  } finally {
    release()
  }
}

function noop () {}

module.exports = DSiteDBTable
