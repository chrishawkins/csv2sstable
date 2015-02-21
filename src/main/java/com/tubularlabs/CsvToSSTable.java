package com.tubularlabs;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;


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
        HashMap<String, Integer> mapping = readMapping(mappingDefinition);
        CFMetaData cfMetaData = getCFMetadata(cqlStatement);
        List<String> mappedFields = new LinkedList<String>(mapping.keySet());
        String insertStatement = getInsertStatement(cfMetaData, mappedFields);

        // magic from original snippet (https://github.com/yukim/cassandra-bulkload-example)
        Config.setClientMode(true);

        // Create output directory that has keyspace and table name in the path
        File outputDir = new File(outputPath + File.separator + cfMetaData.ksName + File.separator + cfMetaData.cfName);
        if (!outputDir.exists() && !outputDir.mkdirs()) {
            throw new RuntimeException("Cannot create output directory: " + outputDir);
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
}
