# Introduction

Glue for data connectors to and from the Snowflake data warehouse.

`snowflow` is a configuration-based framework for simple data flows to and from
Snowflake. Where possible we attempt to provide minimal glue code and leverage
other, more robust tools.  An example use case is importing a filtered list of
tables and columns from an RDS s3 export. `snowflow` handles the actual
Snowflake schema / table creation, type casting, etc.

WARNING: This project is pre-alpha. The structure and API are highly volatile,
and the "framework" aspects are minimal. 

## Terminology

The framework consists of inflows and outflows, where the direction is
with respect to Snowflake. That is, an inflow is a data flow from some external
source (such as s3) into Snowflake. An outflow (none of which currently are
implemented) would consist of an export from Snowflake.

# Configuration

Most parameters are passed to a flow via a framework configuration file
located at `${XDG_CONFIG_HOME}/snowflow/config.yml` or an arbitrary location
specified at configuration load time.  Credentials, source and destination
paths, and table / column filters are all defined in this configuration file.

Configuration sections typically look similar for each unit of data. For
example, a similar set of configurations can be applied at the column, table,
schema, and database level. 

## Filtering

The `snowflow` configuration framework supports basic inclusion / exclusion
filtering based on exact and pattern matches. For example, it is possible to
import only a single column `mycolumn` from table `mytable` from a MySQL schema
(database) `myschema`.

## Commands

Each unit of data (column, table, schema) can be assigned a few different
command that determine the behavior of the flow.  

-   (Default) A `write` command will (re)write the data object regardless of
    previous existence.

-   An `writenx` command will only write the table if `snowflow` detects the
    table or schema does not already exist (either by direct querying, path
    existence, or inspection of run logs.) This can be useful for example when
    a dealing with a large import from s3.

-   A `skip` command will entirely ignore the object. Useful for debugging and
    and alleviates excessive configuration commenting.


## Example

Below is an example configuration section for a MySQL schema that employs
explicit filtering and a command directive to only write a single column from a
single table from the MySQL `schema` if that table does not already exist.


```yaml
name: myschema
type: mysql
filter:
  type: include
  items:
    - mytable
tables:
  - name: mytable
    command: writenx
    filter:
      type: include
        items: 
          - mycolumn
```


## Snowflake Credentials

`snowflow` can look for Snowflake credentials in a SnowSQL config file,
typically located at `~/.snowsql/config`. See
[SnowSQL Config documentation][snowsql-config] for more details.
If no file is found, the environment variables `SNOWSQL_{ACCOUNT,USER,PWD}` are
used.

[snowsql-config]: <https://docs.snowflake.com/en/user-guide/snowsql-config.html#snowsql-config-file> "SnowSQL Config"

