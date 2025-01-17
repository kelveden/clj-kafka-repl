clj-kafka-repl
==============
> This is still a work in progress and should be considered very much "pre-alpha" for now. A lot of the functions are
> currently in an indeterminate state although the likes of `kafka/consumer-chan` and `kafka/get-lag` I've used recently
> and so should be fairly reliable. The whole project is crying out for some proper testing in a lot of places though.

General purpose Clojure REPL functions for interrogating Kafka.

[API](https://kelveden.github.io/clj-kafka-repl/)

Features
--------

The functionality provided in the `kafka` namespace can be split as follows:

* Topic details: [get-earliest-offsets](https://kelveden.github.io/clj-kafka-repl/clj-kafka-repl.kafka.html#var-get-earliest-offsets),
  [get-latest-offsets](https://kelveden.github.io/clj-kafka-repl/clj-kafka-repl.kafka.html#var-get-latest-offsets),
  [get-offsets-at](https://kelveden.github.io/clj-kafka-repl/clj-kafka-repl.kafka.html#var-get-offsets-at),
  [get-topic-partitions](https://kelveden.github.io/clj-kafka-repl/clj-kafka-repl.kafka.html#var-get-topic-partitions),
  [get-topics](https://kelveden.github.io/clj-kafka-repl/clj-kafka-repl.kafka.html#var-get-topics)
* Consumer group offsets: [get-group-offset](https://kelveden.github.io/clj-kafka-repl/clj-kafka-repl.kafka.html#var-get-group-offset),
 [get-group-offsets](https://kelveden.github.io/clj-kafka-repl/clj-kafka-repl.kafka.html#var-get-group-offsets), 
 [set-group-offsets!](https://kelveden.github.io/clj-kafka-repl/clj-kafka-repl.kafka.html#var-set-group-offsets!),
 [get-lag](https://kelveden.github.io/clj-kafka-repl/clj-kafka-repl.kafka.html#var-get-lag)
* Consuming: [consumer-chan](https://kelveden.github.io/clj-kafka-repl/clj-kafka-repl.kafka.html#var-consumer-chan), 
 [sample](https://kelveden.github.io/clj-kafka-repl/clj-kafka-repl.kafka.html#var-sample),
 [get-message](https://kelveden.github.io/clj-kafka-repl/clj-kafka-repl.kafka.html#var-get-message)
* Producing: [producer-chan](https://kelveden.github.io/clj-kafka-repl/clj-kafka-repl.kafka.html#var-producer-chan)

Getting started
---------------

### Configuration

Before starting you'll need to create a configuration file in `~/.clj-kafka-repl/config.edn`. It should be of the form:

```clj
{:profiles
 {:prod
  {:kafka-config
   ; As defined here: https://kafka.apache.org/documentation/#configuration
   {:bootstrap.servers "host1:port1,host2:port2,...",
    ...},

   ; If you wish to use Apache Avro
   :schema-registry-config
   {:base-url "host:port",
    :username "schema-registry-username",
    :password "schema-registry-password"}},

  :nonprod
  {...}}}
``` 

### Running

Once configured, just start a REPL. As indicated at the prompt, just
run `(help)` to get a breakdown of available functions.

When you run one of the functions you will need to specify which profile to use. This is done with the `with` macro; E.g.:

```clj
(with :nonprod (kafka/get-latest-offsets "your-topic"))
```

### Examples



You can find an example configuration and code to run from the REPL at the bottom of [user.clj](./src/user.clj).

Serialization/deserialization
-----------------------------

Several serialization/deserialization formats are supported:

* `:string`: as plain text. This is the default for both keys and values.
* `:edn`: as edn
* `:avro`: Use [Apache Avro](https://avro.apache.org/) and a [Confluent schema registry](https://docs.confluent.io/current/schema-registry/index.html) defined in the configuration. (Serialization/deserialization is provided by the excellent [kafka-avro-confluent](https://github.com/ovotech/kafka-avro-confluent) library.)
* `:nippy`: compressed/decompressed using [nippy](https://github.com/ptaoussanis/nippy).

You can either specify these formats in calls to the functions or in the configuration using the
`:default-key-serializer`, `:default-value-serializer`, `:default-key-deserializer` and `:default-value-deserializer`
top-level configuration options. For example, to serialize/deserialize values using Avro (but continue to
serialize/deserialize as strings):

```clj
{:default-value-serializer   :avro
 :default-value-deserializer :avro

 :profiles {...}}
```

Using Confluent/Schema Registry
-------------------------------

You'll need the `ca.pem`, `service.cert` and `service.key` for a user on Confluent and then you'll need to add them to
a truststore and keystore; e.g.:

```sh
openssl pkcs12 -export -inkey service.key -in service.cert -out client.keystore.p12 -name service_key
keytool -import -file ca.pem -alias CA -keystore client.truststore.jks
```

Then you'll need to configure the kafka client in `~/.clj-kafka-repl/config.edn`:

```clojure
{:default-value-serializer   :avro
 :default-value-deserializer :avro

 :profiles
 {:prod
  {:kafka-config
   {:bootstrap.servers "..."
    :ssl.key.password "..."
    :ssl.keystore.type "pkcs12"
    :ssl.keystore.location "<path to client.keystore.p12>"
    :ssl.keystore.password "..."
    :ssl.truststore.location "<path to client.truststore.jks>"
    :ssl.truststore.password "..."},

   :schema-registry-config
   {:base-url "https://<schema-registry-host>:<port>",
    :username "...",
    :password "..."}}}}
```

Running tests
-------------
> There are *very* limited tests right now - I'm working on it...

```
lein kaocha
```

or, from the REPL itself:

```
(run-tests)
```
