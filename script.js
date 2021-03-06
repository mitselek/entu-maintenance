if (process.env.NEW_RELIC_LICENSE_KEY) { require('newrelic') }

const path    = require('path')
const debug   = require('debug')('app:' + path.basename(__filename).replace('.js', ''))
const op      = require('object-path')
const fs      = require('fs')

const mysql   = require('mysql')
const async = require('async')
const Janitor = require('./janitor.js')

const APP_MYSQL_HOST     = process.env.MYSQL_HOST
const APP_MYSQL_PORT     = process.env.MYSQL_PORT
const APP_MYSQL_DATABASE = process.env.MYSQL_DATABASE
const APP_MYSQL_USER     = process.env.MYSQL_USER
const APP_MYSQL_PASSWORD = process.env.MYSQL_PASSWORD
// APP_CUSTOMERGROUP  = process.env.CUSTOMERGROUP
const APP_CUSTOMERGROUP  = 1332


const APP_ROOT_DIR = path.join(__dirname, '.')
const APP_TIMESTAMP_DIR = path.join(APP_ROOT_DIR, 'timestamps')
if (!fs.existsSync(APP_TIMESTAMP_DIR)) { fs.mkdirSync(APP_TIMESTAMP_DIR) }


debug('creating connection...')
var connection = mysql.createConnection({
  host     : APP_MYSQL_HOST,
  port     : APP_MYSQL_PORT,
  database : APP_MYSQL_DATABASE,
  user     : APP_MYSQL_USER,
  password : APP_MYSQL_PASSWORD
})

debug('connecting...')
connection.connect(function(err) {
  if (err) { throw err }
  debug('connected as id ' + connection.threadId)

  fetch_customers(function(err, customers) {
    if (err) { throw err }
    async.eachOf(customers, function (customer, key, callback) {
      debug('Call for janitor: ', JSON.stringify(customer['database-name'], null, 4))
      let janitor = new Janitor(customer, APP_TIMESTAMP_DIR, callback)
    }, function (err) {
      if (err) { debug(err) }
      debug('done')
    })
  })

  connection.end()
})


var fetch_customers = function(callback) {
  let sql = `
  SELECT DISTINCT
    e.id AS entity,
    property_definition.dataproperty AS property,
    IF(
      property_definition.datatype='decimal',
      property.value_decimal,
      IF(
        property_definition.datatype='integer',
        property.value_integer,
        IF(
          property_definition.datatype='file',
          property.value_file,
          property.value_string
        )
      )
    ) AS value
  FROM (
    SELECT
      entity.id,
      entity.entity_definition_keyname
    FROM
      entity,
      relationship
    WHERE relationship.related_entity_id = entity.id
    AND entity.is_deleted = 0
    AND relationship.is_deleted = 0
    AND relationship.relationship_definition_keyname = 'child'
    AND relationship.entity_id IN (` + APP_CUSTOMERGROUP + `)
  ) AS e
  LEFT JOIN property_definition ON property_definition.entity_definition_keyname = e.entity_definition_keyname AND property_definition.is_deleted = 0
  LEFT JOIN property ON property.property_definition_keyname = property_definition.keyname AND property.entity_id = e.id AND property.is_deleted = 0
  ;
  `

  connection.query(sql, function(err, rows, fields) {
    if (err) {
      callback(err)
      return
    }
    let customers = {}
    rows.forEach(function(row) {
      if (['database-host', 'database-port', 'database-name', 'database-user', 'database-password', 'database-ssl', 'language'].indexOf(row['property']) > -1) {
        op.set(customers, [row['entity'], row['property']], row['value'])
      }
    })
    callback(null, customers)
  })
}
