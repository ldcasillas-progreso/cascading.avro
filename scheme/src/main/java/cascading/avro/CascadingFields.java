package cascading.avro;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.hadoop.io.BytesWritable;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 * The dual to {@link AvroSchemata}: given an Avro {@link Schema}, generate a Cascading {@link Fields} object from it.
 *
 * @author Luis Casillas
 */
public abstract class CascadingFields {

    /**
     * Parse an Avro {@link Schema} and produce a <em>typed</em> Cascading {@link Fields} object from it.
     *
     * @param schema an Avro {@link Schema}
     * @return a <em>typed</em> Cascading {@link Fields} object
     */
    public static Fields generateCascadingFields(Schema schema) {
        List<Schema.Field> avroFields = schema.getFields();
        Comparable[] fieldNames = new Comparable[avroFields.size()];
        Type[] fieldTypes = new Type[avroFields.size()];

        for (int i = 0; i < fieldNames.length; i++) {
            Schema.Field avroField = avroFields.get(i);
            fieldNames[i] = avroField.name();
            fieldTypes[i] = avroTypeToCascadingType(avroField.schema());
        }
        return new Fields(fieldNames, fieldTypes);
    }

    private static Type avroTypeToCascadingType(Schema schema) {
        switch (schema.getType()) {
            case RECORD:
                return TupleEntry.class;

            case ARRAY:
                return List.class;

            case ENUM:
            case STRING:
                return String.class;

            case FIXED:
            case BYTES:
                return BytesWritable.class;

            case MAP:
                return Map.class;

            case NULL:
                return Void.class;

            case BOOLEAN:
                return boolean.class;

            case DOUBLE:
                return double.class;

            case FLOAT:
                return float.class;

            case INT:
                return int.class;

            case LONG:
                return long.class;

            case UNION:
                return avroUnionToCascadingType(schema.getTypes());
            default:
                throw new AvroRuntimeException("Can't convert from type " + schema.getType().toString());
        }
    }

    private static Type avroUnionToCascadingType(List<Schema> schemas) {
        if (schemas.size() < 1) {
            throw new AvroRuntimeException("Union has no types");
        }
        if (schemas.size() == 1) {
            return avroTypeToCascadingType(schemas.get(0));
        } else if (schemas.size() > 2) {
            throw new AvroRuntimeException("Unions may only consist of a concrete type and null in cascading.avro");
        } else if (!schemas.get(0).getType().equals(Schema.Type.NULL) && !schemas.get(1).getType().equals(Schema.Type.NULL)) {
            throw new AvroRuntimeException("Unions may only consist of a concrete type and null in cascading.avro");
        } else {
            int concreteIndex = (schemas.get(0).getType() == Schema.Type.NULL) ? 1 : 0;
            Type raw = avroTypeToCascadingType(schemas.get(concreteIndex));
            return upcaseType(raw);
        }
    }

    private static Type upcaseType(Type raw) {
        if (raw == boolean.class) {
            return Boolean.class;
        } else if (raw == double.class) {
            return Double.class;
        } else if (raw == float.class) {
            return Float.class;
        } else if (raw == int.class) {
            return Integer.class;
        } else if (raw == long.class) {
            return Long.class;
        } else {
            return raw;
        }
    }

}
