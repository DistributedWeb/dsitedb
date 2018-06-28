const tempy = require('tempy')
const DPackVault = require('@dpack/vault')
const DSiteDB = require('../../index')
const {debug, veryDebug} = require('../../lib/util')

var __counter = 0
exports.newDSiteDB = function () {
  const name = 'dSiteDbTest' + (++__counter)
  debug('\n##', name, '\n')
  var dir = tempy.directory()
  veryDebug('DSiteDB dir:', dir)
  return new DSiteDB(dir, {DPackVault})
}

exports.reopenDB = function (db) {
  return new DSiteDB(db.name, {DPackVault})
}

var lastTs = 0
exports.ts = function () {
  var ts = Date.now()
  while (ts <= lastTs) {
    ts++ // cheat to avoid a collision
  }
  lastTs = ts
  return ts
}
