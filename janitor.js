var path    = require('path')
var debug   = require('debug')('app:' + path.basename(__filename).replace('.js', ''))

var fs      = require('fs')
var moment  = require('moment')
var op      = require('object-path')

const async = require('async')
const mysql = require('mysql')

module.exports = function(customer, timestamp_dir, callback) {
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
      debug(err.message)
      return callback()
    }

    // signal back and start monitoring and maintenance for database
    debug('connected to ' + customer['database-name'] + ' as id ' + connection.threadId)
    callback()
    async.until(
      function() { return false },
      function(callback) {
        routine(connection, timestamp_dir, function(err) {
          if (err) { debug(err) }
          setTimeout(function () {
            callback()
          }, 1e4)
        })
      }
    )
  })
}

let routine = function routine(connection, timestamp_dir, callback) {
  debug('Routine for', connection.config.database)

  let timestamp = 1352470566 // 2016-10-19 08:15:45
  let timestamp_filename = path.join(timestamp_dir, connection.config.database)
  if (fs.existsSync(timestamp_filename)) {
    timestamp = fs.readFileSync(timestamp_filename)
  } else {
    fs.writeFileSync(timestamp_filename, timestamp)
  }

  let timestamp_constraint = ' HAVING timestamp > ' + timestamp
  let sort_direction = (timestamp ? 'ASC' : 'DESC')
  let limit = 100

  let changed_entities_sql = `
    SELECT DISTINCT events.definition AS definition, events.id AS id, dates.action AS action, dates.timestamp AS timestamp
    FROM (
      SELECT DISTINCT 'created at'            AS action,
                      Unix_timestamp(created) AS timestamp
      FROM   entity
      WHERE  is_deleted = 0
             ` + timestamp_constraint + `
      UNION ALL
      SELECT DISTINCT 'changed at'            AS action,
                      Unix_timestamp(changed) AS timestamp
      FROM   entity
      WHERE  is_deleted = 0
             ` + timestamp_constraint + `
      UNION ALL
      SELECT DISTINCT 'deleted at'            AS action,
                      Unix_timestamp(deleted) AS timestamp
      FROM   entity
      WHERE  is_deleted = 1
             ` + timestamp_constraint + `
      ORDER  BY timestamp ` + sort_direction + `
      LIMIT ` + limit + `
    ) AS dates
    LEFT JOIN (
      SELECT entity_definition_keyname AS definition,
             id                        AS id,
             'created at'              AS action,
             Unix_timestamp(created)   AS timestamp
      FROM   entity
      WHERE  is_deleted = 0
             ` + timestamp_constraint + `
      UNION ALL
      SELECT entity_definition_keyname AS definition,
             id                        AS id,
             'changed at'              AS action,
             Unix_timestamp(changed)   AS timestamp
      FROM   entity
      WHERE  is_deleted = 0
             ` + timestamp_constraint + `
      UNION ALL
      SELECT entity_definition_keyname AS definition,
             id                        AS id,
             'deleted at'              AS action,
             Unix_timestamp(deleted)   AS timestamp
      FROM   entity
      WHERE  is_deleted = 1
             ` + timestamp_constraint + `
    ) AS events
      ON events.timestamp = dates.timestamp
     AND events.action = dates.action
    ORDER BY dates.timestamp;`

  connection.query(changed_entities_sql, function(err, rows, fields) {
    if (err) { return callback(err) }
    debug('Changes for ' + connection.config.database + ' are ', JSON.stringify(rows, null, 4))
    // connection.end()
    debug('All clean at ' + connection.config.database)
    return callback()
  })
}
