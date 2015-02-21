package com.tubularlabs;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.commons.cli.*;

import java.util.LinkedHashMap;


public class CsvToSSTable {
    public static void main(String[] args) {
        Options options = defineOptions();
        CommandLineParser parser = new GnuParser();

        try {
            CommandLine commandLine = parser.parse(options, args);
            migrate(
                    commandLine.getOptionValue("cql"),
                    commandLine.getOptionValue("mapping"),
                    commandLine.getOptionValue("csv"),
                    commandLine.getOptionValue("output")
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

        return options;
    }

    private static void migrate(String cqlStatement, String mappingDefinition, String csvPath, String outputPath) {
        LinkedHashMap<String, Integer> mapping = readMapping(mappingDefinition);
        CFMetaData cfMetaData = getCFMetadata(cqlStatement);

        System.out.println(mapping);
        System.out.println(cfMetaData);
        System.out.println(csvPath);
        System.out.println(outputPath);
    }

    private static LinkedHashMap<String,Integer> readMapping(String mapping) {
        LinkedHashMap<String, Integer> parsedMapping = new LinkedHashMap<String, Integer>();

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
}
