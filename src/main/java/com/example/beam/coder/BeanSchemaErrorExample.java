package com.example.beam.coder;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

public class BeanSchemaErrorExample {
    public static void main(String[] args) {
        PipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(ExampleOptions.class);
        run(options);
    }

    private static void run(PipelineOptions options) {
        Pipeline pipeline = Pipeline.create(options);
        TestRow r1 = new TestRow();
        r1.setKey("r1");
        r1.setDateField(Instant.ofEpochMilli(1621364350001L));
        List<TestRow> LINES = Collections.singletonList(r1);
        PCollection<TestRow> input = pipeline.apply(Create.of(LINES));

        pipeline.run();
    }

    public interface ExampleOptions extends PipelineOptions {}

    @DefaultSchema(JavaBeanSchema.class)
    public static class TestRow {
        String key;
        Instant dateField;

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public Instant getDateField() {
            return dateField;
        }

        public void setDateField(Instant dateField) {
            this.dateField = dateField;
        }
    }
}
