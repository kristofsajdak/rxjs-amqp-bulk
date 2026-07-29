const Api = require('./lib/api');
const config = require('./config')
const AmqpConnectionFactory = require('./lib/amqp-connection')
const AmqpDestinations = require('./lib/amqp-destinations')
const EsSync = require('./lib/sync')
const Online = require('./lib/online')

const amqpConnection = AmqpConnectionFactory.connect(config)
const esSync = EsSync(config, amqpConnection)
const online = Online(esSync, config, amqpConnection)

AmqpDestinations.setup(config)
    .then(()=> Api.bootstrap(amqpConnection, config).then((server) => server.start()))
    .then(()=> online.pipeline().subscribe())
    .catch(console.error)