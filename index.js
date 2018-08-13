/* globals window */

const EventEmitter = require('events')
const level = require('level-browserify')
const sublevel = require('subleveldown')
const levelPromisify = require('level-promise')
const {debug, veryDebug, assert, getObjectChecksum, URL} = require('./lib/util')
const {SchemaError} = require('./lib/errors')
const TableDef = require('./lib/table-def')
const Indexer = require('./lib/indexer')
const DSiteDBTable = require('./lib/table')
const flatten = require('lodash.flatten')

class DSiteDB extends EventEmitter {
  constructor (name, opts = {}) {
    super()
    if (typeof window === 'undefined' && !opts.DWebVault) {
      throw new Error('Must provide {DWebVault} opt when using DSiteDB outside the browser.')
    }
    this.level = false
    this.name = name
    this.isBeingOpened = false
    this.isOpen = false
    this.DWebVault = opts.DWebVault || window.DWebVault
    this._indexMetaLevel = null
    this._tableSchemaLevel = null
    this._tableDefs = {}
    this._vaults = {}
    this._tablesToRebuild = []
    this._activeSchema = null
    this._tableFilePatterns = []
    this._dbReadyPromise = new Promise((resolve, reject) => {
      this.once('open', () => resolve(this))
      this.once('open-failed', reject)
    })
  }

  async open () {
    // guard against duplicate opens
    if (this.isBeingOpened || this.level) {
      veryDebug('duplicate open, returning ready promise')
      return this._dbReadyPromise
    }
    if (this.isOpen) {
      return
    }
    this.isBeingOpened = true // TODO needed?
    var neededRebuilds = []

    // open the db
    debug('opening')
    try {
      this.level = level(this.name, {valueEncoding: 'json'})
      this._tableSchemaLevel = sublevel(this.level, '_tableSchema', {valueEncoding: 'json'})
      levelPromisify(this._tableSchemaLevel)
      this._indexMetaLevel = sublevel(this.level, '_indexMeta', {valueEncoding: 'json'})
      levelPromisify(this._indexMetaLevel)

      // construct the tables
      const tableNames = Object.keys(this._tableDefs)
      debug('adding tables', tableNames)
      tableNames.forEach(tableName => {
        this[tableName] = new DSiteDBTable(this, tableName, this._tableDefs[tableName])
        this._tableFilePatterns.push(this[tableName]._filePattern)
      })
      this._tableFilePatterns = flatten(this._tableFilePatterns)

      // detect table-definition changes
      for (let i = 0; i < tableNames.length; i++) {
        let tableName = tableNames[i]
        let tableChecksum = this._tableDefs[tableName].checksum

        // load the saved checksum
        let lastChecksum
        try {
          let tableMeta = await this._tableSchemaLevel.get(tableName)
          lastChecksum = tableMeta.checksum
        } catch (e) {}

        // compare
        if (lastChecksum !== tableChecksum) {
          neededRebuilds.push(tableName)
        }
      }

      // run rebuilds
      // TODO go per-table
      await Indexer.resetOutdatedIndexes(this, neededRebuilds)
      this.emit('indexes-reset')

      // save checksums
      for (let i = 0; i < tableNames.length; i++) {
        let tableName = tableNames[i]
        let tableChecksum = this._tableDefs[tableName].checksum
        await this._tableSchemaLevel.put(tableName, {checksum: tableChecksum})
      }

      this.isBeingOpened = false
      this.isOpen = true

      // events
      debug('opened')
      this.emit('open')
    } catch (e) {
      console.error('Upgrade has failed', e)
      this.isBeingOpened = false
      this.emit('open-failed', e)
      throw e
    }

    return {
      rebuilds: neededRebuilds
    }
  }

  async close () {
    if (!this.isOpen) return
    debug('closing')
    this.isOpen = false
    if (this.level) {
      this.listSources().forEach(url => Indexer.unwatchVault(this, this._vaults[url]))
      this._vaults = {}
      await new Promise(resolve => this.level.close(resolve))
      this.level = null
      veryDebug('db .level closed')
    } else {
      veryDebug('db .level didnt yet exist')
    }
  }

  async delete () {
    if (this.isOpen) {
      await this.close()
    }
    await DSiteDB.delete(this.name)
  }

  define (tableName, definition) {
    assert(!this.level && !this.isBeingOpened, SchemaError, 'Cannot define a table when database is open')
    let checksum = getObjectChecksum(definition)
    TableDef.validateAndSanitize(definition)
    definition.checksum = checksum
    this._tableDefs[tableName] = definition
  }

  get tables () {
    return Object.keys(this._tableDefs)
      .filter(name => !name.startsWith('_'))
      .map(name => this[name])
  }

  async indexVault (vault, opts = {}) {
    opts.watch = (typeof opts.watch === 'boolean') ? opts.watch : true

    // handle array case
    if (Array.isArray(vault)) {
      return Promise.all(vault.map(a => this.indexVault(a, opts)))
    }

    // create our own new DWebVault instance
    vault = typeof vault === 'string' ? new (this.DWebVault)(vault) : vault
    debug('DSiteDB.indexVault', vault.url)
    if (!(vault.url in this._vaults)) {
      // store and process
      this._vaults[vault.url] = vault
      await Indexer.addVault(this, vault, opts)
    } else {
      await Indexer.indexVault(this, vault)
    }
  }

  async unindexVault (vault) {
    vault = typeof vault === 'string' ? new (this.DWebVault)(vault) : vault
    if (vault.url in this._vaults) {
      debug('DSiteDB.unindexVault', vault.url)
      delete this._vaults[vault.url]
      await Indexer.removeVault(this, vault)
    }
  }

  async indexFile (vault, filepath) {
    if (typeof vault === 'string') {
      const urlp = new URL(vault)
      vault = new (this.DWebVault)(urlp.protocol + '//' + urlp.hostname)
      return this.indexFile(vault, urlp.pathname)
    }
    await Indexer.readAndIndexFile(this, vault, filepath)
  }

  async unindexFile (vault, filepath) {
    if (typeof vault === 'string') {
      const urlp = new URL(vault)
      vault = new (this.DWebVault)(urlp.protocol + '//' + urlp.hostname)
      return this.indexFile(vault, urlp.pathname)
    }
    await Indexer.unindexFile(this, vault, filepath)
  }

  listSources () {
    return Object.keys(this._vaults)
  }

  isSource (url) {
    if (!url) return false
    if (url.url) url = url.url // an vault
    return (url in this._vaults)
  }

  static list () {
    // TODO
  }

  static delete (name) {
    if (typeof level.destroy !== 'function') {
      throw new Error('Cannot .delete() databases outside of the browser environment. You should just delete the files manually.')
    }

    // delete the database from indexeddb
    return new Promise((resolve, reject) => {
      level.destroy(name, err => {
        if (err) reject(err)
        else resolve()
      })
    })
  }
}
module.exports = DSiteDB
