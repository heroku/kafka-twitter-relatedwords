'use strict';

const fs = require('fs' );

const _ = require('lodash');

// standard JSON library cannot handle size of numeric twitter ID values
const JSONbig = require('json-bigint');

const Bacon = require('baconjs');

const Kafka = require('no-kafka');
const consumerTopicBase = process.env.KAFKA_CONSUMER_TOPIC;

// const configTopic = process.env.KAFKA_CONFIG_TOPIC;

const ONE_SECOND = 1000;

const stopWords = fs.readFileSync('stop-words-final.txt')
                    .toString()
                    .split('\n')
                    .concat([consumerTopicBase, 'rt']);


/*
 * TODO: replace the env var by reading the comma-separated list of terms from the 'config' topic
 *   'config' topic does not yet have terms in it because I haven't figured out how to
 *   write the terms to the 'config' topic on startup of the the 'ingest' producer client
 */
// const potentialKeywords = process.env.TWITTER_TRACK_TERMS.toLowerCase().split(',');
/*
let potentialKeywords = [];
*/

/*
 * Connect to topic as consumer
 * on message,
 *
 */

 // Check that required Kafka environment variables are defined
 const cert = process.env.KAFKA_CLIENT_CERT
 const key  = process.env.KAFKA_CLIENT_CERT_KEY
 const url  = process.env.KAFKA_URL
 if (!cert) throw new Error('KAFKA_CLIENT_CERT environment variable must be defined.');
 if (!key) throw new Error('KAFKA_CLIENT_CERT_KEY environment variable must be defined.');
 if (!url) throw new Error('KAFKA_URL environment variable must be defined.');

 // Write certs to disk because that's how no-kafka library needs them
 fs.writeFileSync('./client.crt', process.env.KAFKA_CLIENT_CERT)
 fs.writeFileSync('./client.key', process.env.KAFKA_CLIENT_CERT_KEY)

// Configure consumer client
const consumer = new Kafka.SimpleConsumer({
    idleTimeout: 100,
    clientId: 'twitter-relatedwords-consumer',
    connectionString: url.replace(/\+ssl/g,''),
    ssl: {
      certFile: './client.crt',
      keyFile: './client.key'
    }
});

// Configure producer client
const producer = new Kafka.Producer({
    clientId: 'tweet-relatedwords-producer',
    connectionString: url.replace(/\+ssl/g,''),
    ssl: {
      certFile: './client.crt',
      keyFile: './client.key'
    }
});

/*
 * Startup producer followed by consumer
 *
 */
return producer.init().then(function() {
    console.log('Producer connected.');
    return consumer.init().then(function () {
        console.log('Consumer connected.');

        const consumerTopic = `${consumerTopicBase}-keyword`;

        console.log('Consuming from topic:', consumerTopic)

        // Create Bacon stream from incoming Kafka messages
        const stream = Bacon.fromBinder(function(sink) {
            function dataHandler(messageSet, topic, partition) {
                messageSet.forEach(function (m) {
                    sink(JSONbig.parse(m.message.value.toString('utf8')).text);
                });
            }
            consumer.subscribe(consumerTopic, dataHandler);

            return function() {
                consumer.unsubscribe(consumerTopic);
            }
        });

        // Beautiful function from here
        // http://stackoverflow.com/questions/30906807/word-frequency-in-javascript
        function wordFreq(accumulator, string) {
          return _.replace(string, /[.!?"'#,():;-]/g, '')
            .split(/\s/)
            .map(word => word.toLowerCase())
            .filter(word => ( !_.includes(stopWords, word) )) //dump words in stop list
            .filter(word => word.match(/.{2,}/)) //dump single char words
            .filter(word => ( !word.match(/\d+/) )) //dump all numeric words
            .filter(word => ( !word.match(/http/) )) //dump words containing http
            .filter(word => ( !word.match(/@/) )) //dump words containing @
            .reduce((map, word) =>
              Object.assign(map, {
                [word]: (map[word])
                    ? map[word] + 1
                  : 1,
              }),
              accumulator
            );
        }

        // Update word frequencies on every message received
        let wordFrequencies = stream.scan({}, wordFreq);

        // Sample word frequencies every second
        let sampledFrequencyStream = wordFrequencies.sample(ONE_SECOND);

        sampledFrequencyStream.onValue(function(frequencies) {
            let sortedKeys = _.keys(frequencies).sort((a,b) => (frequencies[b]-frequencies[a]));

            let mostFrequentWords = {};
            for (let i = 0; i < sortedKeys.length; i++) {
                if (i == 100) break;

                let key = sortedKeys[i];
                let val = frequencies[key];

                mostFrequentWords[key] = val;
            }

            let msg = {
                time: Date.now(),
                relations: mostFrequentWords
            };

            producer.send({
                topic: `${consumerTopicBase}-relatedwords`,
                partition: 0,
                message: {
                    value: JSONbig.stringify(msg)
                }
            })
        });
    });
});
