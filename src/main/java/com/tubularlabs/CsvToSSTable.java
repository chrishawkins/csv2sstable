package com.tubularlabs;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.commons.cli.*;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.*;
import java.util.*;


public class CsvToSSTable {
    static ByteBuffer columnByteBuffer = ByteBuffer.allocate(100 * 1024 * 1024);  // Max column size is 100 MB, useful for list fields
    static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    static byte columnDelimiter = 1;
    static byte listDelimiter = 2;

    public static void main(String[] args) {
        Options options = defineOptions();
        CommandLineParser parser = new GnuParser();

        try {
            CommandLine commandLine = parser.parse(options, args);
            migrate(
                    commandLine.getOptionValue("cql"),
                    commandLine.getOptionValue("mapping"),
                    commandLine.getOptionValue("csv"),
                    commandLine.getOptionValue("output"),
                    commandLine.hasOption("debug")
            );
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("csv2sstable", options);
        }
    }

    private static Options defineOptions() {
        Options options = new Options();

        options.addOption(
                OptionBuilder
                        .withLongOpt("cql")
                        .withDescription("CQL create table statement.")
                        .hasArg()
                        .isRequired()
                        .create()
        );
        options.addOption(
                OptionBuilder
                        .withLongOpt("mapping")
                        .withDescription("Mapping between table fields and csv columns.")
                        .hasArg()
                        .isRequired()
                        .create()
        );
        options.addOption(
                OptionBuilder
                        .withLongOpt("csv")
                        .withDescription("Path to csv file.")
                        .hasArg()
                        .isRequired()
                        .create()
        );
        options.addOption(
                OptionBuilder
                        .withLongOpt("output")
                        .withDescription("Path to dir with sstables.")
                        .hasArg()
                        .isRequired()
                        .create()
        );
        options.addOption(
                OptionBuilder
                        .withLongOpt("debug")
                        .withDescription("Show how mapping will be performed.")
                        .create()
        );

        return options;
    }

    private static void migrate(String cqlStatement, String mappingDefinition, String csvPath, String outputPath, boolean debug) {
        CFMetaData cfMetaData = getCFMetadata(cqlStatement);
        HashMap<String, Integer> mapping = readMapping(mappingDefinition);
        List<String> mappedFields = new LinkedList<String>(mapping.keySet());
        String insertStatement = getInsertStatement(cfMetaData, mappedFields);
        HashMap<String, AbstractType<?>> mappingTypes = readMappingTypes(cfMetaData, mappedFields);

        columnByteBuffer.order(ByteOrder.BIG_ENDIAN);

        // Magic from original snippet (https://github.com/yukim/cassandra-bulkload-example)
        Config.setClientMode(true);

        // Create output directory that has keyspace and table name in the path
        File outputDir = new File(outputPath + File.separator + cfMetaData.ksName + File.separator + cfMetaData.cfName);
        if (!outputDir.exists() && !outputDir.mkdirs()) {
            throw new RuntimeException("Cannot create output directory: " + outputDir);
        }

        // Prepare SSTable writer
        CQLSSTableWriter writer = CQLSSTableWriter
                .builder()
                .inDirectory(outputDir)
                .forTable(cqlStatement)
                .using(insertStatement)
                .build();

        // Read source file
        LineIterator lineIterator;
        try {
            lineIterator = FileUtils.lineIterator(new File(csvPath), "UTF-8");
        } catch (IOException e) {
            throw new RuntimeException("Can't read source csv file.", e);
        }

        List<String> columns;
        List<Object> values = new LinkedList<Object>();
        String csvValue;
        AbstractType<?> valueType;
        Object value;
        int progress = 0;

        while (lineIterator.hasNext()) {
            progress ++;
            columns = splitStringByByte(lineIterator.nextLine(), columnDelimiter);
            
            if (debug)
                System.out.println(String.format("----------------- ROW %d -----------------", progress));

            for (String field: mappedFields) {
                csvValue = columns.get(mapping.get(field));
                valueType = mappingTypes.get(field);
                value = getValue(csvValue, valueType);
                
                if (debug)
                    System.out.println(String.format("Field: %s, Type: %s, Value: %s", field, valueType.getClass().getSimpleName(), value));
                
                values.add(value);
            }

            try {
                writer.addRow(values);
            } catch (IOException e) {
            } catch (InvalidRequestException e) {}

            values.clear();
        }

        try {
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException("Can't close sstable writer.", e);
        }
    }

    private static HashMap<String,Integer> readMapping(String mapping) {
        HashMap<String, Integer> parsedMapping = new HashMap<String, Integer>();

        for (String fieldDefinition: mapping.split(",")) {
            String[] nameAndValue = fieldDefinition.split(":");
            parsedMapping.put(nameAndValue[0], Integer.parseInt(nameAndValue[1]));
        }

        return parsedMapping;
    }

    private static CFMetaData getCFMetadata(String cql) {
        CreateTableStatement statement;
        CFMetaData cfMetaData;

        try {
            statement = (CreateTableStatement) QueryProcessor.parseStatement(cql).prepare().statement;
        } catch (RequestValidationException e) {
            throw new RuntimeException("Error configuring SSTable reader.", e);
        }

        try {
            cfMetaData = statement.getCFMetaData();
        } catch (RequestValidationException e) {
            throw new RuntimeException("Error initializing CFMetadata from CQL.", e);
        }

        return cfMetaData;
    }

    private static String getInsertStatement(CFMetaData cfMetaData, List<String> fields) {
        return String.format(
                "INSERT INTO %s.%s (%s) VALUES (%s)",
                cfMetaData.ksName,
                cfMetaData.cfName,
                StringUtils.join(fields, ", "),
                StringUtils.repeat("?", ", ", fields.size())
        );
    }

    private static HashMap<String,AbstractType<?>> readMappingTypes(CFMetaData cfMetaData, List<String> mappedFields) {
        HashMap<String,AbstractType<?>> mappedTypes = new HashMap<String, AbstractType<?>>();
        for (String field: mappedFields) {
            ColumnDefinition columnDefinition = cfMetaData.getColumnDefinition(ByteBuffer.wrap(field.getBytes()));
            mappedTypes.put(field, columnDefinition.getValidator());
        }

        return mappedTypes;
    }
    
    private static String getStringFromBuffer() {
        columnByteBuffer.flip();
        byte[] column = new byte[columnByteBuffer.limit()];
        columnByteBuffer.get(column);
        columnByteBuffer.clear();
        return new String(column);
    }

    private static List<String> splitStringByByte(String source, byte splitter) {
        List<String> splitString = new ArrayList<String>();

        for (byte oneByte: source.getBytes()) {
            if (oneByte == splitter) {
                splitString.add(getStringFromBuffer());
            } else {
                columnByteBuffer.put(oneByte);
            }
        }

        splitString.add(getStringFromBuffer());
        return splitString;
    }

    private static Object getValue(String csvValue, AbstractType<?> columnType) {
        if (csvValue.length() == 0)
            return null;

        if (columnType instanceof Int32Type)
            return Integer.parseInt(csvValue);
        else if (columnType instanceof IntegerType)
            return new BigInteger(csvValue);
        else if (columnType instanceof LongType)
            return Long.parseLong(csvValue);
        else if (columnType instanceof DoubleType)
            return Double.valueOf(csvValue);
        else if (columnType instanceof FloatType)
            return Float.parseFloat(csvValue);
        else if (columnType instanceof ListType)
            return splitStringByByte(csvValue, listDelimiter);
        else if (columnType instanceof SetType)
            return new HashSet<String>(splitStringByByte(csvValue, listDelimiter));
        else if (columnType instanceof TimestampType) {
            try {
                return dateFormat.parse(csvValue);
            } catch (java.text.ParseException e) {
                return null;
            }
        }

        return csvValue;
    }
}
