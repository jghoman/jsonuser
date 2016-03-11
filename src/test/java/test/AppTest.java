package test;

import example.avro.User;
import org.apache.avro.Schema;
import org.apache.avro.data.Json;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for simple App.
 */
public class AppTest

{
    @Test
    public void TestGenericRecord() throws IOException {
        String schemaString = "{\"namespace\": \"example.avro\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"User\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"name\", \"type\": \"string\"},\n" +
            "     {\"name\": \"favorite_number\",  \"type\": \"int\"},\n" +
            "     {\"name\": \"favorite_color\", \"type\": \"string\"}\n" +
            " ]\n" +
            "}";

        Schema schema = new Schema.Parser().parse(schemaString);

        String json = "{ \"name\" : \"joe\", \"favorite_number\":42, \"favorite_color\":\"blue\"}";

        Decoder decoder = DecoderFactory.get().jsonDecoder(schema, json);

        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        GenericRecord datum = reader.read(null, decoder);
        System.out.println("datum = " + datum);
        System.out.println("name = " + datum.get("name"));
        System.out.println("type = " + datum.getClass().getCanonicalName());
    }


    @Test
    public void TestSpecificRecord() throws IOException {
        String schemaString = "{\"namespace\": \"example.avro\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"User\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"name\", \"type\": \"string\"},\n" +
            "     {\"name\": \"favorite_number\",  \"type\": \"int\"},\n" +
            "     {\"name\": \"favorite_color\", \"type\": \"string\"}\n" +
            " ]\n" +
            "}";

        Schema schema = new Schema.Parser().parse(schemaString);

        String json = "{ \"name\" : \"joe\", \"favorite_number\":42, \"favorite_color\":\"blue\"}";

        Decoder decoder = DecoderFactory.get().jsonDecoder(schema, json);

        DatumReader<SpecificRecord> reader = new SpecificDatumReader<>(schema);
        SpecificRecord datum = reader.read(null, decoder);
        System.out.println("datum = " + datum);
        System.out.println("type = " + datum.getClass().getCanonicalName());
        User asUser = (User)datum;
        System.out.println("asUser = " + asUser.getName());
    }

    public static class DifferentUser {
        private String name;
        private int favoriteNumber;
        private String favoriteColor;

        public DifferentUser() {}

        public DifferentUser(String favoriteColor, int favoriteNumber, String name) {
            this.favoriteColor = favoriteColor;
            this.favoriteNumber = favoriteNumber;
            this.name = name;
        }

        public String getFavoriteColor() {
            return favoriteColor;
        }

        public void setFavoriteColor(String favoriteColor) {
            this.favoriteColor = favoriteColor;
        }

        public int getFavoriteNumber() {
            return favoriteNumber;
        }

        public void setFavoriteNumber(int favoriteNumber) {
            this.favoriteNumber = favoriteNumber;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    @Test
    public void TestReflection() throws IOException {
       Schema schema = ReflectData.get().getSchema(DifferentUser.class);

        System.out.println("schema = " + schema.toString(true));

        GenericRecord gr = new GenericRecordBuilder(schema)
            .set("name", "joe")
            .set("favoriteNumber", 42)
            .set("favoriteColor", "blue")
            .build();

        GenericDatumWriter<GenericRecord> gdw = new GenericDatumWriter<>(schema);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
        gdw.write(gr, encoder);
        encoder.flush();


        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        Decoder decoder = DecoderFactory.get().binaryDecoder(bais, null);

        ReflectDatumReader<DifferentUser> rdr = new ReflectDatumReader<>(DifferentUser.class);
        DifferentUser differentUser = rdr.read(null, decoder);
        assertEquals("joe", differentUser.getName());
        assertEquals(42, differentUser.getFavoriteNumber());
        assertEquals("blue", differentUser.getFavoriteColor());
    }


    @Test
    @Ignore
    public void TestArbitraryJson() throws IOException {
        String schemaString = "{\"namespace\": \"example.avro\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"User\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"name\", \"type\": \"string\"},\n" +
            "     {\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]},\n" +
            "     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}\n" +
            " ]\n" +
            "}";

        Schema schema = new Schema.Parser().parse(schemaString);

        String json = "{ \"name\" : \"joe\", \"favorite_number\":42, \"favorite_color\":\"blue\"}";

        Json.ObjectReader objectReader = new Json.ObjectReader();
        objectReader.setSchema(schema);
        Decoder decoder = DecoderFactory.get().jsonDecoder(schema, json);
        Object z = objectReader.read(null, decoder);
        System.out.println("z = " + z);
        System.out.println("z class = " + z.getClass().getCanonicalName());
    }
}
