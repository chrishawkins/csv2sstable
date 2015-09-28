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
import java.nio.CharBuffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.*;
import java.util.*;


public class CsvToSSTable {
    static CharBuffer columnByteBuffer = CharBuffer.allocate(1024 * 1024);
    static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    static char columnDelimiter = ',';
    static char quoteChar = '"';
    static String listDelimiter = ",";

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
            columns = splitStringAccountForQuotes(lineIterator.nextLine(), columnDelimiter);
            
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
        char[] column = new char[columnByteBuffer.limit()];
        columnByteBuffer.get(column);
        columnByteBuffer.clear();
        return new String(column);
    }

    private static List<String> splitStringAccountForQuotes(String source, char splitter) {
        List<String> splitString = new ArrayList<String>();
        boolean inQuotes = false;

        for (char character: source.toCharArray()) {
            if (character == splitter && !inQuotes) {
                splitString.add(getStringFromBuffer());
            } else if (character == quoteChar) {
                inQuotes = !inQuotes;
            } else {
                columnByteBuffer.put(character);
            }
        }

        splitString.add(getStringFromBuffer());
        return splitString;
    }

    private static HashMap<String, Double> getHashMap(Collection<String> list) {

        HashMap<String, Double> map = new HashMap<String, Double>();

        for (String pair : list) {
            String[] bits = pair.split(":");
            String key = bits[0].trim().replace("'", "");
            double value = Double.parseDouble(bits[1].trim());
            map.put(key, value);
        }

        return map;
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
            return csvValue.split(listDelimiter);
        else if (columnType instanceof SetType)
            return new HashSet<String>(Arrays.asList(csvValue.split(listDelimiter)));
        else if (columnType instanceof MapType)
            return getHashMap(Arrays.asList(csvValue.substring(1, csvValue.length() - 2).split(listDelimiter)));
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
