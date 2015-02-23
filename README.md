# csv2sstable

Tool for loading Sqoop csv files into Cassandra.

## Installing

    ./gradlew installDist && sudo cp -R build/install/csv2sstable/* /usr

## Usages
You need to specify cql create statement for table and mapping between csv columns and table columns.

    csv2sstable --cql="create table mykeyspace.mycolumnfamilly (id text, count int, price float, PRIMATY KEY (id))" \
                --mapping="id:0,count:1,price:2" \
                --csv="path/to/source.csv" \
                --output="path/to/output/dir"
                
You also can use option `--debug` to understand how csv columns will be mapped to sstable fields.

## Development
For development you can use:

    ./run.sh --cql=... --mapping=... --csv=... --output=...

## Notes
This tool requires refactoring and it will be done later.
Also, some additional features are planned:

- support for all complex types (currently only list<text> and set<text> are supported)
- wrapper for running this converter on Hadoop
