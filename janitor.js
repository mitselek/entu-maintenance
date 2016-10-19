var path    = require('path')
var debug   = require('debug')('app:' + path.basename(__filename).replace('.js', ''))

var fs      = require('fs')
var moment  = require('moment')
var op      = require('object-path')

const mysql = require('mysql')

module.exports = function(customer, callback) {
  debug('creating connection for ', JSON.stringify(customer, null, 4))

  let connection = mysql.createConnection({
    // host     : customer['database-host'],
    host     : '127.0.0.1',
    port     : customer['database-port'],
    database : customer['database-name'],
    user     : customer['database-user'],
    password : customer['database-password']
  })

  debug('connecting to ' + customer['database-name'] + '...')

  connection.connect(function(err) {
    if (err) {
      debug(err)
      return callback()
    }
    debug('connected to ' + customer['database-name'] + ' as id ' + connection.threadId)

    connection.end()

    debug('All clean at ' + customer['database-name'])

    return callback()
  })

}
