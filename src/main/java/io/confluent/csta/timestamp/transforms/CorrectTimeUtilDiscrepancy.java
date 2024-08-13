package io.confluent.csta.timestamp.transforms;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class CorrectTimeUtilDiscrepancy<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Check if date field has value specified as in java.time and if is different than java.util one replace with that value";

    private interface ConfigName {
        String FIELD_NAME = "field.name";
        String FIELD_VALUE = "field.value";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.FIELD_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                    "date field to check")
            .define(ConfigName.FIELD_VALUE, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                    "date field value to check");

    private static final String PURPOSE = "Workaround for java.time and java.util representation discrepancy of older dates";

    private static final Logger logger = LoggerFactory.getLogger(CorrectTimeUtilDiscrepancy.class);


    private String fieldName;
    private String fieldValue;
    private Date problematicDateValue;
    private Date correctedDate;
    private boolean possiblyProblematic;

    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldName = config.getString(ConfigName.FIELD_NAME);
        fieldValue = config.getString(ConfigName.FIELD_VALUE);
        problematicDateValue = getProblematicFieldValueInJavaUtil(fieldValue);
        try {
            correctedDate = new SimpleDateFormat("yyyy-MM-dd").parse(fieldValue);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        possiblyProblematic= !problematicDateValue.equals(correctedDate);


        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
    }

    static Date getProblematicFieldValueInJavaUtil(String dateValue){
        return new Date(getDaysForJavaTime(dateValue)* 24 * 60 * 60 * 1000);
    }

    static long getDaysForJavaTime(String dateValue){
        LocalDate date = LocalDate.parse(dateValue);
        return ChronoUnit.DAYS.between(LocalDate.ofEpochDay(0),date);
    }


    @Override
    public R apply(R record) {

        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
        final Map<String, Object> updatedValue = new HashMap<>(value);

        if (isProblematic(value.get(fieldName))) {
            updatedValue.replace(fieldName, correctedDate);
        }

        return newRecord(record, null, updatedValue);
    }

    private boolean isProblematic(Object value){

        return possiblyProblematic && value instanceof Date && ((Date)value).equals(problematicDateValue);
    }


    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);
        logger.debug("Original Record: " + record.toString());
        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if(updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }
        logger.debug( "New updated Schema " + updatedSchema.fields().toString());
        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : value.schema().fields()) {
            updatedValue.put(field.name(), value.get(field));
        }

        if (isProblematic(value.get(fieldName))) {
            updatedValue.put(fieldName, correctedDate);
        }

        return newRecord(record, updatedSchema, updatedValue);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (Field field: schema.fields()) {
            builder.field(field.name(), field.schema());
        }

        return builder.build();
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends CorrectTimeUtilDiscrepancy<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

    }

    public static class Value<R extends ConnectRecord<R>> extends CorrectTimeUtilDiscrepancy<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }

    }
}

