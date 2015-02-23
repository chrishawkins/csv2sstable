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

## Development
For development you can use:

    ./gradlew run --cql=... --mapping=... --csv=... --output=...
