FROM gcr.io/dataflow-templates-base/java8-template-launcher-base:latest

# Define the Java command options required by Dataflow Flex Templates.
ENV FLEX_TEMPLATE_JAVA_MAIN_CLASS="terekete.beam.template.BigQueryToGcs"
ENV FLEX_TEMPLATE_JAVA_CLASSPATH="/template/pipeline.jar"

# Make sure to package as an uber-jar including all dependencies.
COPY ./bigquery-to-gcs-1.0-SNAPSHOT.jar ${FLEX_TEMPLATE_JAVA_CLASSPATH}
