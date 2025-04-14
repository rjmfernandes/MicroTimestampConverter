# Custom Timestamp Converter SMT

Currently TimestampConverter looses precision beyond milliseconds during sinking to an external database. We present here a first exploration of the problem and possible workarounds.

- [Custom Timestamp Converter SMT](#custom-timestamp-converter-smt)
  - [Setup](#setup)
    - [Start Docker Compose](#start-docker-compose)
    - [Install JDBC Sink Connector plugin](#install-jdbc-sink-connector-plugin)
  - [Microseconds Precision Loss](#microseconds-precision-loss)
    - [Reproduce the issue](#reproduce-the-issue)
    - [Custom SMT and DB Trigger](#custom-smt-and-db-trigger)
    - [No Custom SMT just DB Trigger Workaround](#no-custom-smt-just-db-trigger-workaround)
  - [Source JDBC Connector](#source-jdbc-connector)
    - [Default Behaviour](#default-behaviour)
    - [TimestampConverter SMT](#timestampconverter-smt)
    - [Large Values Issue](#large-values-issue)
    - [SMT Chain with Custom SMT Workaround](#smt-chain-with-custom-smt-workaround)
  - [With Date](#with-date)
    - [Source JDBC](#source-jdbc)
    - [Sink after](#sink-after)
    - [Sorting java.time and java.util discrepancy before sinking with custom SMT](#sorting-javatime-and-javautil-discrepancy-before-sinking-with-custom-smt)
    - [Tombstone](#tombstone)
    - [Making sure topic has java.time type of values when using JDBC Source Connector for `0001-01-01`](#making-sure-topic-has-javatime-type-of-values-when-using-jdbc-source-connector-for-0001-01-01)
  - [Cleanup](#cleanup)

## Setup

If you want to make sure to test with last versions of connectors run first:

```bash
rm -fr ./plugins/confluentinc-kafka-connect-jdbc
```

### Start Docker Compose

```bash
docker compose up -d
```

Check logs for confirming all services are running:

```bash
docker compose logs -f
```

### Install JDBC Sink Connector plugin

```bash
docker compose exec -it connect bash
```

Once inside the container we can install a new connector from confluent-hub:

```bash
confluent-hub install confluentinc/kafka-connect-jdbc:latest
```

(Choose option 2 and after say yes to everything when prompted.)

Now we need to restart our connect:

```bash
docker compose restart connect
```

Now if we list our plugins we should see two new ones corresponding to the JDBC connector.

```bash
curl localhost:8083/connector-plugins | jq
```

## Microseconds Precision Loss

### Reproduce the issue

Lets register our schema against Schema Registry:

```bash
jq '. | {schema: tojson}' src/main/resources/avro/customer.avsc | \
curl -X POST http://localhost:8081/subjects/customers-value/versions \
-H "Content-Type: application/vnd.schemaregistry.v1+json" \
-d @-
```

Now let's run our producer io.confluent.csta.timestamp.avro.AvroProducer.

And check with consumer:

```bash
kafka-avro-console-consumer --topic customers \
--bootstrap-server 127.0.0.1:9092 \
--property schema.registry.url=http://127.0.0.1:8081 \
--from-beginning
```

Just some entries should be enough so you can stop after a while.

Now let's create our sink connector:

```bash
curl -i -X PUT -H "Accept:application/json" \
    -H  "Content-Type:application/json" http://localhost:8083/connectors/my-sink-postgres/config \
    -d '{
          "connector.class"    : "io.confluent.connect.jdbc.JdbcSinkConnector",
          "connection.url"     : "jdbc:postgresql://host.docker.internal:5432/postgres",
          "connection.user"    : "postgres",
          "connection.password": "password",
          "topics"             : "customers",
          "tasks.max"          : "1",
          "auto.create"        : "true",
          "auto.evolve"        : "true",
          "value.converter.schema.registry.url": "http://schema-registry:8081",
          "value.converter.schemas.enable":"false",
          "key.converter"       : "org.apache.kafka.connect.storage.StringConverter",
          "value.converter"     : "io.confluent.connect.avro.AvroConverter",
            "transforms": "timesmod",
            "transforms.timesmod.field": "customer_time",
            "transforms.timesmod.target.type": "Timestamp",
            "transforms.timesmod.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
            "transforms.timesmod.unix.precision": "microseconds"}'
```

If we check our database and look for the table customer rows we will see the entries keep only milliseconds resolution.

The issue is that currently TimestampConverter relies on java.util.Date and SimpleDateFormat both with resolution till milliseconds.

### Custom SMT and DB Trigger

Create a new table in postgres:

```sql
create table customers2 (first_name text, last_name text, customer_time text,
						 customer_time_final timestamp without time zone);
```

You will also need to create a trigger:

```sql
CREATE FUNCTION time_stamp() RETURNS trigger AS $time_stamp$
    BEGIN
        IF NEW.customer_time IS NULL THEN
            RAISE EXCEPTION 'customer_time cannot be null';
        END IF;
        NEW.customer_time_final := TO_TIMESTAMP(SUBSTRING(NEW.customer_time,1,26),'yyyy-MM-dd HH24:MI:SS.US');
        RETURN NEW;
    END;
$time_stamp$ LANGUAGE plpgsql;

CREATE TRIGGER time_stamp BEFORE INSERT OR UPDATE ON customers2
    FOR EACH ROW EXECUTE FUNCTION time_stamp();
```

Now let's try to use our custom SMT:

```bash
curl -i -X PUT -H "Accept:application/json" \
    -H  "Content-Type:application/json" http://localhost:8083/connectors/my-sink-postgres2/config \
    -d '{
          "connector.class"    : "io.confluent.connect.jdbc.JdbcSinkConnector",
          "connection.url"     : "jdbc:postgresql://host.docker.internal:5432/postgres",
          "connection.user"    : "postgres",
          "connection.password": "password",
          "topics"             : "customers",
          "table.name.format"  : "${topic}2",
          "tasks.max"          : "1",
          "auto.create"        : "true",
          "auto.evolve"        : "true",
          "value.converter.schema.registry.url": "http://schema-registry:8081",
          "value.converter.schemas.enable":"false",
          "key.converter"       : "org.apache.kafka.connect.storage.StringConverter",
          "value.converter"     : "io.confluent.connect.avro.AvroConverter",
            "transforms": "timesmod",
            "transforms.timesmod.field": "customer_time",
            "transforms.timesmod.target.type": "string",
            "transforms.timesmod.type": "io.confluent.csta.timestamp.transforms.TimestampConverterMicro$Value",
             "transforms.timesmod.format": "yyyy-MM-dd HH:mm:ss.nnnnnn",
            "transforms.timesmod.unix.precision": "microseconds"}'
```

This way it should get populated the new table `customers2` with micro seconds resolution.

This custom SMT class `io.confluent.csta.timestamp.transforms.TimestampConverterMicro` is an example of an implementation leveraging java.time.Instant and DateTimeFormatter so 'in principle' allowing for higher resolution. But unfortunately the target type Timestamp cannot be directy used cause Connect internally still expects in such case a java.util.Date and not java.time.Instant... So we keep in this example the implementation as it is, as an exploration example, even if a target type as Timestamp won't work with it right now.

But we leverage the string target type (with a format value we can use here but not applicable for SimpleDateFormat used in old standard TimestampConverter) and a trigger on database side to workaround the issue.

### No Custom SMT just DB Trigger Workaround

Create a new table in posgres:

```sql
create table customers3 (first_name text, last_name text, customer_time bigint,
						 customer_time_final timestamp without time zone);
```

And the corresponding trigger:

```sql
CREATE FUNCTION time_stamp3() RETURNS trigger AS $time_stamp3$
    BEGIN
        -- Check that empname and salary are given
        IF NEW.customer_time IS NULL THEN
            RAISE EXCEPTION 'customer_time cannot be null';
        END IF;

        -- Remember who changed the payroll when
        NEW.customer_time_final := to_timestamp(NEW.customer_time::double precision/1000/1000);
        RETURN NEW;
    END;
$time_stamp3$ LANGUAGE plpgsql;

CREATE TRIGGER time_stamp3 BEFORE INSERT OR UPDATE ON customers3
    FOR EACH ROW EXECUTE FUNCTION time_stamp3();
```

Now let's create the connector with no SMT:

```shell
curl -i -X PUT -H "Accept:application/json" \
    -H  "Content-Type:application/json" http://localhost:8083/connectors/my-sink-postgres3/config \
    -d '{
          "connector.class"    : "io.confluent.connect.jdbc.JdbcSinkConnector",
          "connection.url"     : "jdbc:postgresql://host.docker.internal:5432/postgres",
          "connection.user"    : "postgres",
          "connection.password": "password",
          "topics"             : "customers",
          "table.name.format"  : "${topic}3",
          "tasks.max"          : "1",
          "auto.create"        : "true",
          "auto.evolve"        : "true",
          "value.converter.schema.registry.url": "http://schema-registry:8081",
          "value.converter.schemas.enable":"false",
          "key.converter"       : "org.apache.kafka.connect.storage.StringConverter",
          "value.converter"     : "io.confluent.connect.avro.AvroConverter"}'
```

This way it should get populated the new table `customers3` with micro seconds resolution.

## Source JDBC Connector 

### Default Behaviour

Let's create the table:

```sql
create table customers100 (first_name text not null, last_name text not null,customer_time timestamp without time zone not null);
```

Let's insert a value on it:

```sql
INSERT INTO customers100(
	first_name, last_name, customer_time)
	VALUES ('rui', 'fernandes',   now());
```

Define a JDBC source connector for table customers3:

```shell
curl -i -X PUT -H "Accept:application/json" \
     -H "Content-Type: application/json" http://localhost:8083/connectors/my-source-postgres/config \
     -d '{
             "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
             "connection.url": "jdbc:postgresql://host.docker.internal:5432/postgres",
             "connection.user": "postgres",
             "connection.password": "password",
             "topic.prefix": "postgres-",
             "poll.interval.ms" : 3600000,
             "table.whitelist" : "customers100",
          "timestamp.granularity": "nanos_long",
             "mode":"bulk"}'
```

You can see the topic `postgres-customers100` getting created with schema:

```json
{
  "connect.name": "customers100",
  "fields": [
    {
      "name": "first_name",
      "type": "string"
    },
    {
      "name": "last_name",
      "type": "string"
    },
    {
      "name": "customer_time",
      "type": "long"
    }
  ],
  "name": "customers100",
  "type": "record"
}
```

Also checking the messages of the topic:

```json
{
  "first_name": "rui",
  "last_name": "fernandes",
  "customer_time": "1719780510259008000"
}
```

Which is the original time with microseconds precision now with nanoseconds (the last 3 zeros) but it does not have the right timestamp logical type.

### TimestampConverter SMT

Next we could try to use default timestamp converter for the field and see what happens in each case:

```shell
curl -i -X PUT -H "Accept:application/json" \
     -H "Content-Type: application/json" http://localhost:8083/connectors/my-source2-postgres/config \
     -d '{
             "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
             "connection.url": "jdbc:postgresql://host.docker.internal:5432/postgres",
             "connection.user": "postgres",
             "connection.password": "password",
             "topic.prefix": "postgres2-",
             "poll.interval.ms" : 3600000,
             "table.whitelist" : "customers100",
             "mode":"bulk",
             "value.converter.schema.registry.url": "http://schema-registry:8081",
          "value.converter.schemas.enable":"false",
          "key.converter"       : "org.apache.kafka.connect.storage.StringConverter",
          "value.converter"     : "io.confluent.connect.avro.AvroConverter",
          "timestamp.granularity": "nanos_long",
          "transforms": "timesmod",
            "transforms.timesmod.field": "customer_time",
            "transforms.timesmod.target.type": "Timestamp",
            "transforms.timesmod.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
            "transforms.timesmod.unix.precision": "microseconds"}'
```

In this case we end up with the following schema:

```json
{
  "connect.name": "customers100",
  "fields": [
    {
      "name": "first_name",
      "type": "string"
    },
    {
      "name": "last_name",
      "type": "string"
    },
    {
      "name": "customer_time",
      "type": {
        "connect.name": "org.apache.kafka.connect.data.Timestamp",
        "connect.version": 1,
        "logicalType": "timestamp-millis",
        "type": "long"
      }
    }
  ],
  "name": "customers100",
  "type": "record"
}
```

And message like this:

```json
{
  "first_name": "rui",
  "last_name": "fernandes",
  "customer_time": 1719780510259008
}
```

We have now microseconds resolution but schema is created with millis. We can evolve the schema to use micros resolution:

```json
{
  "connect.name": "customers100",
  "fields": [
    {
      "name": "first_name",
      "type": "string"
    },
    {
      "name": "last_name",
      "type": "string"
    },
    {
      "name": "customer_time",
      "type": {
        "connect.name": "org.apache.kafka.connect.data.Timestamp",
        "connect.version": 1,
        "logicalType": "timestamp-micros",
        "type": "long"
      }
    }
  ],
  "name": "customers100",
  "type": "record"
}
```

And if we restart the connector we get the message still with micros as:

```json
{
  "first_name": "rui",
  "last_name": "fernandes",
  "customer_time": 1719780510259008
}
```

### Large Values Issue

Let's insert a much bigger date on it:

```sql
INSERT INTO customers100(
	first_name, last_name, customer_time)
	VALUES ('rui', 'fernandes',    timestamp '9999-12-31 23:59:59.000000');
```

Now if we restart our connector. We get a message:

```json
{
  "first_name": "rui",
  "last_name": "fernandes",
  "customer_time": "9223372036854775"
}
```

As one can see we get now a string invalid as per our schema in comparison to what we had before which was a valid long representing the micros. 

**This issue does not happen anymore with latest versions of the connectors. As of 20250414. But the value although not a string is still truncated as 9223372036854775.**

The reason for that is that if we transform our date in database to long we get in milliseconds:

```sql
SELECT *,EXTRACT(EPOCH FROM customer_time)*1000 from customers100;
```

```csv
"first_name","last_name","customer_time","?column?"
"rui","fernandes","2024-06-30 20:48:30.259008","1719780510259.01"
"rui","fernandes","9999-12-31 23:59:59","253402300799000"
```

Which means that for the large date we have in nanoseconds `253402300799000000000` which is above `9223372036854775807` the maximum long allowed. The connector basically brings down the possible max value in nanos to micros removing its final 3 decimals and registers the value as string `"9223372036854775"`.

Curious enough for our tests we find the limit to be not exactly around the `9223372036854775807` (which would be around `Friday, 11 April 2262 23:47:16.854` if you check https://www.epochconverter.com/ for `9223372036854775807`) but a bit before for the "change to string" behaviour. If we create two entries:

```sql
INSERT INTO customers100(
	first_name, last_name, customer_time)
	VALUES ('rui', 'fernandes',    timestamp '2255-04-11 23:47:16.853000');

INSERT INTO customers100(
	first_name, last_name, customer_time)
	VALUES ('rui', 'fernandes',    timestamp '2256-04-11 23:47:16.855000');
```

And restart our connector.

We find the first one to be transformed correctly into `9002447236853000` while the second (and any above) into a string, in this case `"9034069636855000"`. Although it still process the microseconds correctly (at least until it reaches the maximum value for long in nanoseconds as mentioned before when it can process any further).

**As mentioned before this issue does not happen anymore with latest versions of the connectors. As of 20250414. But the value although not a string is still truncated as 9223372036854775 for values bigger than the max handled as long. No point doing what's mentioned next since the transformation to string has been fixed.**

It's not clear to us what triggers this behaviour: starting processing as string even before reaching the limit. In any case what we can try is to transform the value into a long after for the cases it is a string. We can do that by changing our connector with smt chain:

```shell
curl -i -X PUT -H "Accept:application/json" \
     -H "Content-Type: application/json" http://localhost:8083/connectors/my-source2-postgres/config \
     -d '{
             "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
             "connection.url": "jdbc:postgresql://host.docker.internal:5432/postgres",
             "connection.user": "postgres",
             "connection.password": "password",
             "topic.prefix": "postgres2-",
             "poll.interval.ms" : 3600000,
             "table.whitelist" : "customers100",
             "mode":"bulk",
             "value.converter.schema.registry.url": "http://schema-registry:8081",
          "value.converter.schemas.enable":"false",
          "key.converter"       : "org.apache.kafka.connect.storage.StringConverter",
          "value.converter"     : "io.confluent.connect.avro.AvroConverter",
          "timestamp.granularity": "nanos_long",
          "transforms": "timesmod,tolong,timesmod2",
            "transforms.timesmod.field": "customer_time",
            "transforms.timesmod.target.type": "Timestamp",
            "transforms.timesmod.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
            "transforms.timesmod.unix.precision": "microseconds",
            "transforms.tolong.field": "tolong",
            "transforms.tolong.type": "org.apache.kafka.connect.transforms.Cast$Value",
            "transforms.tolong.spec": "customer_time:int64",
            "transforms.timesmod2.field": "customer_time",
            "transforms.timesmod2.target.type": "Timestamp",
            "transforms.timesmod2.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
            "transforms.timesmod2.unix.precision": "microseconds"
            }'
```

With this we get for both `9002447236853` and `9034069636855`. But we still can't handle the case of max value `9999-12-31 23:59:59.000000` which will be capped to `Friday, 11 April 2262 23:47:16.854`.

### SMT Chain with Custom SMT Workaround

We have built a custom SMT `io.confluent.csta.timestamp.transforms.InsertMaxDate` that basically checks for dates in the field specified (in our case this will be `customer_time`) for values corresponding to the "nano long max" up to milliseconds (check discussion before) `9223372036854` and replace for those cases by the value `253402300799000` corresponding to our desired max `9999-12-31 23:59:59.000000` in milliseconds.

Let's try to use our custom SMT after the same SMT chain before.

```shell
curl -i -X PUT -H "Accept:application/json" \
     -H "Content-Type: application/json" http://localhost:8083/connectors/my-source3-postgres/config \
     -d '{
             "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
             "connection.url": "jdbc:postgresql://host.docker.internal:5432/postgres",
             "connection.user": "postgres",
             "connection.password": "password",
             "topic.prefix": "postgres3-",
             "poll.interval.ms" : 3600000,
             "table.whitelist" : "customers100",
             "mode":"bulk",
             "value.converter.schema.registry.url": "http://schema-registry:8081",
          "value.converter.schemas.enable":"false",
          "key.converter"       : "org.apache.kafka.connect.storage.StringConverter",
          "value.converter"     : "io.confluent.connect.avro.AvroConverter",
          "timestamp.granularity": "nanos_long",
          "transforms": "timesmod,tolong,timesmod2,setmax",
            "transforms.timesmod.field": "customer_time",
            "transforms.timesmod.target.type": "Timestamp",
            "transforms.timesmod.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
            "transforms.timesmod.unix.precision": "microseconds",
            "transforms.tolong.field": "tolong",
            "transforms.tolong.type": "org.apache.kafka.connect.transforms.Cast$Value",
            "transforms.tolong.spec": "customer_time:int64",
            "transforms.timesmod2.field": "customer_time",
            "transforms.timesmod2.target.type": "Timestamp",
            "transforms.timesmod2.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
            "transforms.timesmod2.unix.precision": "microseconds",
            "transforms.setmax.field.name": "customer_time",
            "transforms.setmax.type": "io.confluent.csta.timestamp.transforms.InsertMaxDate$Value"
            }'
```

We get now what we were looking for the max value (in micros):

```json
{
  "first_name": "rui",
  "last_name": "fernandes",
  "customer_time": 253402300799000
}
```

Let's try now to sink this back into the database:

```shell
curl -i -X PUT -H "Accept:application/json" \
    -H  "Content-Type:application/json" http://localhost:8083/connectors/my-sink-postgres100/config \
    -d '{
          "connector.class"    : "io.confluent.connect.jdbc.JdbcSinkConnector",
          "connection.url"     : "jdbc:postgresql://host.docker.internal:5432/postgres",
          "connection.user"    : "postgres",
          "connection.password": "password",
          "topics"             : "postgres3-customers100",
          "tasks.max"          : "1",
          "auto.create"        : "true",
          "auto.evolve"        : "true",
          "value.converter.schema.registry.url": "http://schema-registry:8081",
          "value.converter.schemas.enable":"false",
          "key.converter"       : "org.apache.kafka.connect.storage.StringConverter",
          "value.converter"     : "io.confluent.connect.avro.AvroConverter",
            "transforms": "timesmod",
            "transforms.timesmod.field": "customer_time",
            "transforms.timesmod.target.type": "Timestamp",
            "transforms.timesmod.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
            "transforms.timesmod.unix.precision": "microseconds"}'
```

If we check the database:

```sql
select * from "postgres3-customers100";
```

We see we are able to keep the results as they were although we lost the micros resolution cause of the sink issue first discussed here.

## With Date 

### Source JDBC

Let's create a table in postgres:

```sql
create table with_date (name text not null, my_date DATE not null);
```

After we insert a row:

```sql
INSERT INTO with_date (name,my_date) VALUES ('Rui','0001-01-01');
```

Now we create a source jdbc connector:

```shell
curl -i -X PUT -H "Accept:application/json" \
     -H "Content-Type: application/json" http://localhost:8083/connectors/with-date/config \
     -d '{
             "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
             "connection.url": "jdbc:postgresql://host.docker.internal:5432/postgres",
             "connection.user": "postgres",
             "connection.password": "password",
             "topic.prefix": "postgres-",
             "poll.interval.ms" : 3600000,
             "table.whitelist" : "with_date",
             "mode":"bulk",
             "value.converter.schema.registry.url": "http://schema-registry:8081",
          "value.converter.schemas.enable":"false",
          "key.converter"       : "org.apache.kafka.connect.storage.StringConverter",
          "value.converter"     : "io.confluent.connect.avro.AvroConverter",
          "transforms": "timesmod",
            "transforms.timesmod.field": "my_date",
            "transforms.timesmod.target.type": "Date",
            "transforms.timesmod.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value"}'
```

You get in the topic:

```json
{
  "name": "Rui",
  "my_date": -719164
}
```

Which you can check https://www.epochconverter.com/seconds-days-since-y0 and see that corresponds in fact to date `Saturday, 30 December 0000` representing what it seems to be 2 days before our original date in database `'0001-01-01'`.

We can check the schema and we get:

```json
{
  "connect.name": "with_date",
  "fields": [
    {
      "name": "name",
      "type": "string"
    },
    {
      "name": "my_date",
      "type": {
        "connect.name": "org.apache.kafka.connect.data.Date",
        "connect.version": 1,
        "logicalType": "date",
        "type": "int"
      }
    }
  ],
  "name": "with_date",
  "type": "record"
}
```

### Sink after

Now we create a connector to sink to database:

```shell
curl -i -X PUT -H "Accept:application/json" \
    -H  "Content-Type:application/json" http://localhost:8083/connectors/with-date-sink/config \
    -d '{
          "connector.class"    : "io.confluent.connect.jdbc.JdbcSinkConnector",
          "connection.url"     : "jdbc:postgresql://host.docker.internal:5432/postgres",
          "connection.user"    : "postgres",
          "connection.password": "password",
          "topics"             : "postgres-with_date",
          "tasks.max"          : "1",
          "auto.create"        : "true",
          "auto.evolve"        : "true",
          "value.converter.schema.registry.url": "http://schema-registry:8081",
          "value.converter.schemas.enable":"false",
          "key.converter"       : "org.apache.kafka.connect.storage.StringConverter",
          "value.converter"     : "io.confluent.connect.avro.AvroConverter",
            "transforms": "timesmod",
            "transforms.timesmod.field": "my_date",
            "transforms.timesmod.target.type": "Date",
            "transforms.timesmod.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value"}'
```

If we execute:

```sql
select * from "postgres-with_date";
```

We see we get in fact same date `'0001-01-01'`.

Everything is coherent aside that check we did on https://www.epochconverter.com/seconds-days-since-y0 for the value stored in the topic.

If we run our class `io.confluent.csta.timestamp.transforms.DaysBeforeEpochComparison` we can see for java.util package classes the representation in days for `'0001-01-01'` is in fact `-719164` while for `java.time` is in fact `-719162`. (This discrepancy doesn't for modern dates as the test class shows.) Basically the web page is using an implementation like java.time classes so the values differ. But the implementation of Kafka is coherent in itself. Meaning, once sourced it will sink the right value after.

The root cause of the discrepancy related to differences for dates before the transition from Julian to Gregorian calendar between `java.util` and `java.time` packages implementation.

The issue will of course appear if the producer to the topic is not our connector but some other implementation `java.time` like... This would send to the topic `-719162` which from the point of view of Kafka would be `'0001-01-03'` (`java.util` based), and if we sink with JDBC connector after that's what it will write on database. 

Next we present a workaround to this problem meanwhile the JDBC connector still uses a `java.util` based implementation (**and if it's not possible for you to adapt your producer/CDC to use also those `java.util` package classes when writing to Kafka**).

### Sorting java.time and java.util discrepancy before sinking with custom SMT

We reproduce the issue arising from a producer using `java.time` representation as `-719162` for `'0001-01-01'` by inserting a date on our database like `'0000-12-30'`. We will suppose it came from a producer/CDC using `java.time` conversion for the date `'0001-01-01'`. The objective here is just trying to reproduce a topic with messages equivalent to what would happen in such scenario.

```sql
create table with_date2 (name text not null, my_date DATE not null);
INSERT INTO with_date2 (name,my_date) VALUES ('Rui', '0001-01-03');
INSERT INTO with_date2 (name,my_date) VALUES ('Rui', '2024-07-07');
```

The first date inserted will generate the equivalent of the problematic date `0001-01-01` if coming from a `java.time` like producer/CDC implementation. While the second is unproblematic `'2024-07-07'`. 

If we now create a connector:

```shell
curl -i -X PUT -H "Accept:application/json" \
     -H "Content-Type: application/json" http://localhost:8083/connectors/with-date3/config \
     -d '{
             "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
             "connection.url": "jdbc:postgresql://host.docker.internal:5432/postgres",
             "connection.user": "postgres",
             "connection.password": "password",
             "topic.prefix": "postgres3-",
             "poll.interval.ms" : 3600000,
             "table.whitelist" : "with_date2",
             "mode":"bulk",
             "value.converter.schema.registry.url": "http://schema-registry:8081",
          "value.converter.schemas.enable":"false",
          "key.converter"       : "org.apache.kafka.connect.storage.StringConverter",
          "value.converter"     : "io.confluent.connect.avro.AvroConverter",
          "transforms": "timesmod",
            "transforms.timesmod.field": "my_date",
            "transforms.timesmod.target.type": "Date",
            "transforms.timesmod.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value"}'
```

We get the problematic message in the topic we wanted to reproduce:

```json
{
  "name": "Rui",
  "my_date": -719162
}
```

While for the second entry we get an unproblematic representation (equal in `java.util` and `java.time`):

```json
{
  "name": "Rui",
  "my_date": 19911
}
```

We can then use a sink connector with our custom SMT `io.confluent.csta.timestamp.transforms.CorrectTimeUtilDiscrepancy` for correcting the discrepancy:

```shell
curl -i -X PUT -H "Accept:application/json" \
    -H  "Content-Type:application/json" http://localhost:8083/connectors/with-date-sink2/config \
    -d '{
          "connector.class"    : "io.confluent.connect.jdbc.JdbcSinkConnector",
          "connection.url"     : "jdbc:postgresql://host.docker.internal:5432/postgres",
          "connection.user"    : "postgres",
          "connection.password": "password",
          "topics"             : "postgres3-with_date2",
          "tasks.max"          : "1",
          "auto.create"        : "true",
          "auto.evolve"        : "true",
          "value.converter.schema.registry.url": "http://schema-registry:8081",
          "value.converter.schemas.enable":"false",
          "key.converter"       : "org.apache.kafka.connect.storage.StringConverter",
          "value.converter"     : "io.confluent.connect.avro.AvroConverter",
            "transforms": "timesmod,correctDays",
            "transforms.timesmod.field": "my_date",
            "transforms.timesmod.target.type": "Date",
            "transforms.timesmod.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
            "transforms.correctDays.field.name": "my_date",
            "transforms.correctDays.field.value": "0001-01-01",
            "transforms.correctDays.type": "io.confluent.csta.timestamp.transforms.CorrectTimeUtilDiscrepancy$Value"}'
```

If we execute:

```sql
select * from "postgres3-with_date2";
```

Now we get on database the value `0001-01-01` for the first message while for the second the value is unproblematic and stays unchanged as `2024-07-07`.

Basically the custom SMT specifically checks (for the field specified) for values that match the value for date passed in the `java.time` representation, and if that value does not corresponds to the representation in `java.util`, identifies the discrepancy and "corrects" to the `java.util` representation.

**Note: This SMT class is meant to serve as an example (while still generic enough) that you can adapt to your specific needs in relation to this issue assuming the date that will generate the issue is quite specific. Considering is uncommon in business scenarios to use such old dates it will generally correspond to a default value signalling the field has not in fact been set.**

### Tombstone

Lets register our schema against Schema Registry:

```bash
jq '. | {schema: tojson}' src/main/resources/avro/customer.avsc | \
curl -X POST http://localhost:8081/subjects/compacted-value/versions \
-H "Content-Type: application/vnd.schemaregistry.v1+json" \
-d @-
```

Create compacted topic:

```bash
kafka-topics --bootstrap-server localhost:9092 --topic compacted --create --partitions 1 --replication-factor 1 --config cleanup.policy=compact
```

Run the class `io.confluent.csta.timestamp.avro.AvroCompactedProducer` and after check the messages:

```bash
kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic compacted --from-beginning --property schema.registry.url=http://localhost:8081 --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=io.confluent.kafka.serializers.KafkaAvroDeserializer --property print.key=true --property print.value=true
```

You should get something like:

```
Rui	{"first_name":"Rui","last_name":"Fernandes","customer_time":1722331211191656}
Carmen	{"first_name":"Carmen","last_name":"Monteiro","customer_time":1722331211467569}
Rui	null
Carmen	{"first_name":"Carmen","last_name":"Fernandes","customer_time":1722331211467758}
```

Let's create our table in Postgres:

```sql
create table compacted (first_name text, last_name text, customer_time timestamp without time zone,PRIMARY KEY(first_name));
```

If we now create a sink connector:

```bash
curl -i -X PUT -H "Accept:application/json" \
    -H  "Content-Type:application/json" http://localhost:8083/connectors/compacted-sink2/config \
    -d '{
          "connector.class"    : "io.confluent.connect.jdbc.JdbcSinkConnector",
          "connection.url"     : "jdbc:postgresql://host.docker.internal:5432/postgres",
          "connection.user"    : "postgres",
          "connection.password": "password",
          "topics"             : "compacted",
          "tasks.max"          : "1",
          "auto.create"        : "false",
          "auto.evolve"        : "true",
          "value.converter.schema.registry.url": "http://schema-registry:8081",
          "value.converter.schemas.enable":"false",
          "key.converter"       : "org.apache.kafka.connect.storage.StringConverter",
          "value.converter"     : "io.confluent.connect.avro.AvroConverter",
            "transforms": "timesmod,correctDays",
            "transforms.timesmod.field": "customer_time",
            "transforms.timesmod.target.type": "Timestamp",
            "transforms.timesmod.unix.precision": "microseconds",
            "transforms.timesmod.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
            "transforms.correctDays.field.name": "customer_time",
            "transforms.correctDays.field.value": "0001-01-01",
            "transforms.correctDays.type": "io.confluent.csta.timestamp.transforms.CorrectTimeUtilDiscrepancy$Value"}'
```



You will get an error similar to the following related to the tombstone record:

```
connect  | Caused by: org.apache.kafka.connect.errors.DataException: Only Map objects supported in absence of schema for [Workaround for java.time and java.util representation discrepancy of older dates], found: null
connect  | 	at org.apache.kafka.connect.transforms.util.Requirements.requireMap(Requirements.java:38)
connect  | 	at io.confluent.csta.timestamp.transforms.CorrectTimeUtilDiscrepancy.applySchemaless(CorrectTimeUtilDiscrepancy.java:92)
connect  | 	at io.confluent.csta.timestamp.transforms.CorrectTimeUtilDiscrepancy.apply(CorrectTimeUtilDiscrepancy.java:85)
connect  | 	at org.apache.kafka.connect.runtime.TransformationStage.apply(TransformationStage.java:57)
connect  | 	at org.apache.kafka.connect.runtime.TransformationChain.lambda$apply$0(TransformationChain.java:54)
connect  | 	at org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator.execAndRetry(RetryWithToleranceOperator.java:180)
connect  | 	at org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator.execAndHandleError(RetryWithToleranceOperator.java:214)
connect  | 	... 15 more
```

This error is not specific of our custom SMT and will happen with other SMTs as well. The root cause is the second tombstone entry for Rui.

We want to use predicates to filter the execution of SMT for the records we are interested in. Also setting delete.enabled to true, insert.mode to upsert, and the pk details.

```bash
curl -i -X PUT -H "Accept:application/json" \
    -H  "Content-Type:application/json" http://localhost:8083/connectors/compacted-sink2/config \
    -d '{
          "connector.class"    : "io.confluent.connect.jdbc.JdbcSinkConnector",
          "connection.url"     : "jdbc:postgresql://host.docker.internal:5432/postgres",
          "connection.user"    : "postgres",
          "connection.password": "password",
          "topics"             : "compacted",
          "tasks.max"          : "1",
          "auto.create"        : "false",
          "auto.evolve"        : "true",
          "delete.enabled"     : "true",
          "insert.mode"        : "upsert",
          "pk.mode"            : "record_key",
          "pk.fields"          : "first_name",
          "value.converter.schema.registry.url": "http://schema-registry:8081",
          "value.converter.schemas.enable":"false",
          "key.converter"       : "org.apache.kafka.connect.storage.StringConverter",
          "value.converter"     : "io.confluent.connect.avro.AvroConverter",
            "transforms": "timesmod,correctDays",
            "transforms.timesmod.field": "customer_time",
            "transforms.timesmod.target.type": "Timestamp",
            "transforms.timesmod.unix.precision": "microseconds",
            "transforms.timesmod.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
            "transforms.correctDays.field.name": "customer_time",
            "transforms.correctDays.field.value": "0001-01-01",
            "transforms.correctDays.type": "io.confluent.csta.timestamp.transforms.CorrectTimeUtilDiscrepancy$Value",
            "transforms.correctDays.predicate": "isNullRecord",
            "transforms.correctDays.negate"   : "true",
            "predicates"                          : "isNullRecord",
            "predicates.isNullRecord.type"        : "org.apache.kafka.connect.transforms.predicates.RecordIsTombstone"}'
```

You should get at the end on the database table:

```sql
select * from compacted;
```

And we get the last update for the Carmen entry for the table:

```
"Carmen"	"Fernandes"	"2024-07-30 09:20:11.467"
```

### Making sure topic has java.time type of values when using JDBC Source Connector for `0001-01-01`

Now we may face the issue where the values loaded from JDBC Source connector need to have the value for `0001-01-01` 
compatible with `java.time`. Even is using JDBC Sink later we have to use the custom SMT 
`io.confluent.csta.timestamp.transforms.CorrectTimeUtilDiscrepancy` before also.

To reproduce we create a table in postgres:

```sql
create table with_date10 (name text not null, my_date DATE not null);
INSERT INTO with_date10 (name,my_date) VALUES ('Rui', '0001-01-01');
INSERT INTO with_date10 (name,my_date) VALUES ('Rui', '2024-07-07');
```

If we now create a connector:

```shell
curl -i -X PUT -H "Accept:application/json" \
     -H "Content-Type: application/json" http://localhost:8083/connectors/with-date10/config \
     -d '{
             "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
             "connection.url": "jdbc:postgresql://host.docker.internal:5432/postgres",
             "connection.user": "postgres",
             "connection.password": "password",
             "topic.prefix": "postgres10-",
             "poll.interval.ms" : 3600000,
             "table.whitelist" : "with_date10",
             "mode":"bulk",
             "value.converter.schema.registry.url": "http://schema-registry:8081",
          "value.converter.schemas.enable":"false",
          "key.converter"       : "org.apache.kafka.connect.storage.StringConverter",
          "value.converter"     : "io.confluent.connect.avro.AvroConverter",
          "transforms": "timesmod",
            "transforms.timesmod.field": "my_date",
            "transforms.timesmod.target.type": "Date",
            "transforms.timesmod.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value"}'
```

We get the problematic message in the topic we wanted to reproduce:

```json
{
  "name": "Rui",
  "my_date": -719164
}
```

We want to guarantee that this value is in fact `-719162` the value for the calendar over `java.time`. So we will need to use a custom SMT.

```shell
curl -i -X PUT -H "Accept:application/json" \
     -H "Content-Type: application/json" http://localhost:8083/connectors/with-date22/config \
     -d '{
             "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
             "connection.url": "jdbc:postgresql://host.docker.internal:5432/postgres",
             "connection.user": "postgres",
             "connection.password": "password",
             "topic.prefix": "postgres22-",
             "poll.interval.ms" : 3600000,
             "table.whitelist" : "with_date10",
             "mode":"bulk",
             "value.converter.schema.registry.url": "http://schema-registry:8081",
          "value.converter.schemas.enable":"false",
          "key.converter"       : "org.apache.kafka.connect.storage.StringConverter",
          "value.converter"     : "io.confluent.connect.avro.AvroConverter",
          "transforms": "correcttime",
          "transforms.correcttime.field.name": "my_date",
            "transforms.correcttime.field.value": "0001-01-01",
            "transforms.correcttime.type": "io.confluent.csta.timestamp.transforms.ReverseCorrectTimeUtilDiscrepancy$Value"}'
```

Now we get on the topic (the `java.time` value):

```json
{
  "name": "Rui",
  "my_date": -719162
}
```

If we wanted to sink now we would need to use our previous SMT:

```bash
curl -i -X PUT -H "Accept:application/json" \
    -H  "Content-Type:application/json" http://localhost:8083/connectors/withdate-sink22/config \
    -d '{
          "connector.class"    : "io.confluent.connect.jdbc.JdbcSinkConnector",
          "connection.url"     : "jdbc:postgresql://host.docker.internal:5432/postgres",
          "connection.user"    : "postgres",
          "connection.password": "password",
          "topics"             : "postgres22-with_date10",
          "tasks.max"          : "1",
          "auto.create"        : "true",
          "auto.evolve"        : "true",
          "value.converter.schema.registry.url": "http://schema-registry:8081",
          "value.converter.schemas.enable":"false",
          "key.converter"       : "org.apache.kafka.connect.storage.StringConverter",
          "value.converter"     : "io.confluent.connect.avro.AvroConverter",
            "transforms": "correctDays",
            "transforms.correctDays.field.name": "my_date",
            "transforms.correctDays.field.value": "0001-01-01",
            "transforms.correctDays.type": "io.confluent.csta.timestamp.transforms.CorrectTimeUtilDiscrepancy$Value"}'
```

And so on database we get as originally:

```
"Rui"	"0001-01-01 00:00:00"
"Rui"	"2024-07-07 00:00:00"
```

## Cleanup

From the root of the project:

```bash
docker compose down -v
```
