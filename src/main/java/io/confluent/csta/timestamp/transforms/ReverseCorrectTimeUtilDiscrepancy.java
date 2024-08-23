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

import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class ReverseCorrectTimeUtilDiscrepancy<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Check if date field has value specified as in java.util and if is different than java.time one replace with that value";

    private interface ConfigName {
        String FIELD_NAME = "field.name";
        String FIELD_VALUE = "field.value";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.FIELD_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                    "date field to check")
            .define(ConfigName.FIELD_VALUE, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                    "date field value to check");

    private static final String PURPOSE = "Guarantees topic has value as per java.time and not java.util of older dates";

    private static final Logger logger = LoggerFactory.getLogger(ReverseCorrectTimeUtilDiscrepancy.class);


    private String fieldName;
    private String fieldValue;
    private long correcteddays;
    private long problematicdays;
    private boolean possiblyProblematic;

    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldName = config.getString(ConfigName.FIELD_NAME);
        fieldValue = config.getString(ConfigName.FIELD_VALUE);
        try {
            correcteddays = getDaysForJavaUtil(fieldValue);
        } catch (ParseException e) {
            logger.error("Error parsing date value: " + fieldValue);
            throw new RuntimeException(e);
        }
        problematicdays = getDaysForJavaTime(fieldValue);
        possiblyProblematic= problematicdays != correcteddays;


        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
    }


    public static void main(String[] args) throws ParseException {
        System.out.println( getDaysForJavaUtil("0001-01-01") );//-719164
        System.out.println( getDaysForJavaTime("0001-01-01") );//-719162
    }

    static long getDaysForJavaUtil(String dateValue) throws ParseException {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new SimpleDateFormat("yyyy-MM-dd").parse(dateValue));
        return (-1)*getDaysBeforeEpochUtil(calendar);
    }

    static long getDaysBeforeEpochUtil(Calendar dateCalendar) {
        Calendar epochCalendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        epochCalendar.clear();  // Clears all fields
        epochCalendar.set(1970, Calendar.JANUARY, 1, 0, 0, 0);
        epochCalendar.set(Calendar.MILLISECOND, 0);  // Ensure no milliseconds

        long differenceInMillis = epochCalendar.getTimeInMillis() - dateCalendar.getTimeInMillis();
        return differenceInMillis / (1000 * 60 * 60 * 24);
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
            updatedValue.replace(fieldName, getJavaSQlDateFromDays(correcteddays));
        }

        return newRecord(record, null, updatedValue);
    }

    private boolean isProblematic(Object value){
        long days=getDaysFromSQLDate((Date) value);
        return possiblyProblematic && value instanceof java.sql.Date && days== problematicdays;
    }

    static long getDaysFromSQLDate(Date date){
        return date.toLocalDate().toEpochDay();
    }


    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);
        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if(updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }
        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : value.schema().fields()) {
            updatedValue.put(field.name(), value.get(field));
        }
        if (isProblematic(value.get(fieldName))) {
            //when is recreated with java.util will get the java.util date
            updatedValue.put(fieldName, getJavaSQlDateFromDays(problematicdays));
        }

        return newRecord(record, updatedSchema, updatedValue);
    }


    static Date getJavaSQlDateFromDays(long days){
        long millis = days * 24L * 60L * 60L * 1000L;
        return new Date(millis);
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

    public static class Key<R extends ConnectRecord<R>> extends ReverseCorrectTimeUtilDiscrepancy<R> {

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

    public static class Value<R extends ConnectRecord<R>> extends ReverseCorrectTimeUtilDiscrepancy<R> {

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

