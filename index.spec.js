const chai = require('chai')
const expect = chai.expect

const Promise = require('bluebird')
const jackrabbit = require('jackrabbit')
const _ = require('lodash')
const Rx = require('rx')
const url = require('url')
const amqp = require('jackrabbit/node_modules/amqplib');
const spies = require('chai-spies')

Promise.longStackTraces()
chai.config.includeStack = true
chai.use(spies);

const dockerHostName = url.parse(process.env.DOCKER_HOST).hostname

describe('Rxjs AMQP', ()=> {

    beforeEach((done)=> {
        rabbit = jackrabbit(`amqp://${dockerHostName}:5672`)
        rabbit.on('connected', ()=> {
            const connection = rabbit.getInternals().connection;
            connection.createChannel(function (err, ch) {
                ch.purgeQueue('hello', (err, ok)=> {
                    if (ok) {
                        ch.close(()=> {
                            exchange = rabbit.default()
                            exchange.on('ready', done)
                        })
                    }
                })
            });
        })

    });

    afterEach((done)=> {
        rabbit.close(done)
    })

    it('converts a jackrabbit queue into an observable', (done)=> {

        const queue = exchange.queue({name: 'hello'})
        const consume = _.partialRight(queue.consume, {noAck: true})

        rabbitToObservable(consume)
            .subscribe(
                (event)=> {
                    if (event.data === `4`) {
                        done()
                    }
                },
                (err) => {
                    done(err)
                }
            );

        _.range(5).map((i)=> `${i}`).forEach((i) => exchange.publish(i, {key: 'hello'}))
    })

    it('should execute the afterBuffer function with the results and ack the source messages', (done)=> {

        const range = _.range(0, 5).map((i)=> `${i}`)

        const mapFn = (event) => {
            return Promise.delay(1).then(()=> {
                return _.merge({}, event, {data: 'a' + event.data})
            })
        }

        const ack = chai.spy(()=> {})
        const nack = chai.spy(()=> {})

        const rabbitObservable = Rx.Observable.create((observer) => {
            range.forEach((i) => {
                observer.onNext({
                    data: i,
                    ack,
                    nack,
                    msg: {}
                })
            })
        })

        var subscription = reliableBufferedObservable(rabbitObservable, mapFn, (results)=> {
            expect(results).to.have.length(5)
            const expected = _.map(range.slice(0, 5), (item)=> {
                return 'a' + item
            })
            expect(expected).to.eql(_.map(results, 'data'))
            return Promise.resolve()
        })

        subscription
            .do((reflectedResults) => {
                    expect(ack).to.have.been.called.exactly(5);
                    expect(nack).to.have.been.called.exactly(0);
                    done()
                },
                done
            )
            .subscribe()

        range.forEach((i) => exchange.publish(i, {key: 'hello'}))
    })

    it('should not pass down the failures to the afterBuffer function and nack the source messages', (done)=> {

        const range = _.range(0, 5).map((i)=> `${i}`)

        const mapFn = (event) => {
            return Promise.delay(1).then(()=> {
                if (_.toNumber(event.data) < 3) {
                    return _.merge({}, event, {data: 'a' + event.data})
                } else {
                    throw new Error('foobar')
                }
            })
        }

        const ack = chai.spy(()=> {})
        const nack = chai.spy(()=> {})

        const rabbitObservable = Rx.Observable.create((observer) => {
            range.forEach((i) => {
                observer.onNext({
                    data: i,
                    ack,
                    nack,
                    msg: {}
                })
            })
        })

        const subscription = reliableBufferedObservable(rabbitObservable, mapFn, (results)=> {
            expect(results).to.have.length(3)
            return Promise.resolve()
        })

        subscription
            .do((reflectedResults) => {
                    expect(ack).to.have.been.called.exactly(3);
                    expect(nack).to.have.been.called.exactly(2);
                    done()
                },
                done
            )
            .subscribe()


    })

    function reliableBufferedObservable(rabbitObservable, mapFn, afterBufferFn) {
        return rabbitObservable
            .map((event) => {
                return {
                    source: event,
                    result: mapFn(event)
                }
            })
            .bufferWithTimeOrCount(5000, 5)
            .flatMap((eventsWithResult)=> {

                var resultPromises = _.map(eventsWithResult, 'result');
                var reflectedResults = resultPromises.map(function (promise) {
                    return promise.reflect();
                });

                const fulfilledPromises = []

                return Promise.all(reflectedResults)
                    .map(function (reflectedResult, index) {
                        if (reflectedResult.isFulfilled()) {
                            fulfilledPromises.push(reflectedResult.value())
                        } else {
                            console.error('promise rejected', reflectedResult.reason());
                            eventsWithResult[index].source.nack()
                        }
                        return reflectedResult
                    })
                    .then((reflectedResults)=> {
                        return afterBufferFn(fulfilledPromises)
                            .then(()=> {
                                fulfilledPromises.forEach((fulfilledPromise)=> {
                                    fulfilledPromise.ack()
                                })
                                return reflectedResults
                            })
                    })
            });
    }

    function rabbitToObservable(consume) {

        return Rx.Observable.create((observer) => {
            consume((data, ack, nack, msg) => {
                observer.onNext({data, ack, nack, msg})
            })
        })
    }

})


