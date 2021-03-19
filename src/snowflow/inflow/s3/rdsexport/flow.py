from __future__ import annotations

import json
import logging
import os
from copy import deepcopy
from pathlib import Path
from typing import Union

import arrow
import jinja2
import jinjasql
import snowflake.connector

from .config import Config

logger = logging.getLogger(__name__)

# TODO:
# - [ ] Inspect actual tables instead of just looking at logs to determine writenx.


class Templates:
    """Template handler. Renders and prepares queries."""

    jsql = jinjasql.JinjaSql()
    template_env = jinja2.Environment(
        # TODO: do not hardcode the package name
        loader=jinja2.PackageLoader(__package__, "templates")
    )

    @classmethod
    def get(cls, name: str) -> jinja2.Template:
        return cls.template_env.get_template(name)

    @classmethod
    def query(cls, name: str, context: dict, prepare=True):
        """Render a template with the input name and context variables.

        Args:
            name: Input template name.
            context: Context to render template.
            prepare: If true, prepare the query using jinjasql.
        """
        template = cls.get(name)
        if prepare:
            query, params = cls.jsql.prepare_query(template, context)
        else:
            query = template.render(context)
            params = []
        return query, params


class SchemaImportTask:
    """Import tables for a specific RDS instance schema.

    The task will record a completion log of the form
        {
            "start": "when task began",
            "end": "when task finished",
            "success": True,
            "tables: {
                "table_name" : {
                    "name": "table name",
                    "start": "when the table writing began",
                    "end": "when the copy to Snowflake completed.",
                    "success": True,
                    "message": "any error message for failure",
                },
                # ...
            },
        }
    Since dicts are insertion order preserving as of python 3.7,the tables
    values should be temporarlly ordered with respect to write time.

    Attrs:
        conf: Configuration node corresponding to the RDS schema.
    """

    def __init__(self, conf: dict):
        """
        Args:
            conf: A configuration node corresponding to the schema. Should
                also include the `s3` key from the instance configuration
                node.
        """
        self.conn = None
        self.instance: str = conf["instance"]
        self.s3: dict = conf["s3"]
        self.schema: str = conf["name"]
        self.snowflake: dict = conf["snowflake"]
        self.conf = conf
        force = self.conf.get("force", False)

        # Load logs.
        self.logpath = Path(
            os.getenv("XDG_DATA_HOME", "~/.local/share"),
            "snowflow",
            "logs",
            f"{self.s3['prefix']}.json",
        )
        self.logpath.parent.mkdir(parents=True, exist_ok=True)
        self.log = self.load_log(ignore=force)

    def connect(self) -> SchemaImportTask:
        self.conn = snowflake.connector.connect(**self.snowflake)
        return self

    def load_log(self, ignore: bool = False) -> dict:
        if not ignore and self.logpath.exists():
            with self.logpath.open() as logfile:
                log = json.load(logfile)
        else:
            log = {
                "success": False,
                "tables": {},
            }
        return log

    def save_log(self):
        with self.logpath.open("w") as logfile:
            json.dump(self.log, logfile)

    def execute(self, query: str, params, execute: bool = True):
        """Execute a query."""
        if execute:
            with self.conn.cursor() as cur:
                cur.execute(query, params)
            self.conn.commit()

    def create_database(self, database: dict, execute: bool = True):
        context = {"database": database}
        query, params = Templates.query("create_database.j2.sql", context)
        self.execute(query, params, execute)
        return query, params

    def create_schema(self, execute: bool = True):
        context = {
            "snowflake": self.snowflake,
            "instance": self.instance,
            "schema": self.schema,
        }
        query, params = Templates.query("create_schema.j2.sql", context)
        self.execute(query, params, execute)
        return query, params

    def use_schema(self, execute: bool = True):
        context = {"schema": self.schema}
        query, params = Templates.query("use_schema.j2.sql", context)
        self.execute(query, params, execute)
        return query, params

    def create_stage(self, execute: bool = True):
        """Creates a temporary Snowflake stage so data can be copied from
        the snapshot export location into Snowflake. The stage is
        associated with a Snowflake DB, schema and points to the prefix
        in an s3 export containing tables for the specified RDS schema.

        Args:
            schema: RDS schema. Needs to be a field in the configuration
                "schemas" mapping.
        """
        context = {
            "s3": self.s3,
            "snowflake": self.snowflake,
            "schema": self.schema,
        }
        query, params = Templates.query("create_stage.j2.sql", context)
        logger.info(
            f"Creating Snowflake import stage for {self.snowflake['database']}.{self.snowflake['schema']}."
        )
        self.execute(query, params, execute)
        return query, params

    def create_table(self, table: str, execute: bool = True):
        table_conf: dict = self.conf["tables"][table]
        snowflake = self.snowflake | table_conf["snowflake"]
        context = {
            "snowflake": snowflake,
            "table": table_conf,
            "instance": self.instance,
            "schema": self.schema,
            "s3": self.s3,
        }
        query, params = Templates.query("create_table.j2.sql", context)
        logger.info(
            f"Creating Snowflake table {snowflake['database']}.{snowflake['schema']}.{snowflake['table']}."
        )
        if execute:
            self.execute(query, params)
        return query, params

    def write_table(self, table: str, execute: bool = True):
        """Writes exported table data into a Snowflake table, which will be
        created if necessary. The appropriate database, schema, table, and (temporary) stage
        need to have been created previously.
        """
        table_conf = self.conf["tables"][table]
        snowflake = self.snowflake | table_conf["snowflake"]
        context = {
            "snowflake": snowflake,
            "table": table,
            "schema": self.schema,
        }
        query, params = Templates.query("copy_into_table.j2.sql", context)
        self.execute(query, params, execute)
        return query, params

    def run(self):
        """Import the tables for the underlying RDS instance schema.
        - Creates the assoiated Snowflake database / schema if necessary.
        - Creates all tables for the schema (after filtering).
        - Copies data from the RDS export into each created Snowflake table.

        TODO: Better connection closing during errors.

        """
        src = f"{self.instance}.{self.schema}"
        tar = f'{self.snowflake["database"]}.{self.snowflake["schema"]}'
        logger.info(f"Writing {src} to {tar}.")

        if self.conf.get("command", "write") == "skip":
            logger.info("Skipping schema write.")
            self.log["command"] = "skip"
            self.log["success"] = True
            return self.log

        self.log["start"] = arrow.utcnow().isoformat()

        self.connect()
        self.create_database(self.snowflake["database"])
        self.create_schema()
        self.use_schema()
        self.create_stage()

        self.log["success"] = True
        for table_name, table_conf in self.conf["tables"].items():
            start = arrow.utcnow()
            src = f"{self.schema}.{table_name}"
            cmd = table_conf.get("command", "writenx")
            entry = self.log["tables"].setdefault(
                table_name,
                {
                    "table": table_name,
                    "start": arrow.utcnow().isoformat(),
                    "success": False,
                    "message": None,
                    "command": cmd,
                },
            )

            # Skip if necessary
            if cmd == "skip":
                entry["success"] = True
                entry["command"] = "skip"
                continue
            elif cmd == "writenx" and entry["success"]:
                logger.info(f"Skipping previously written table {src}.")
                continue

            logger.info(f"Writing RDS table {src} to Snowflake.")
            try:
                self.create_table(table_name)
                self.write_table(table_name)
            except Exception as exc:
                entry["success"] = False
                entry["message"] = str(exc)
                self.log["success"] = False
                logger.warning(
                    f"Unable to write {self.instance}.{self.schema}.{table_name}: {exc}"
                )
            else:
                entry["success"] = True
                entry["end"] = arrow.utcnow().isoformat()
            took = arrow.utcnow() - start
            logger.info(f"Finished writing RDS {src} to Snowflake. Took {took}.")

        self.conn.close()
        self.log["end"] = arrow.utcnow().isoformat()
        self.save_log()
        logger.info(f"Finished writing schema {self.instance}.{self.schema}")
        return self.log


class Flow:
    """
    Import tables for a single RDS snapshot export in s3.

    A SnapshotImporter will parse the snapshot metadata in order to
    create the appropriate Snowflake schemas, tables, ingestion stages,
    etc.

    Attrs:
    """

    # TODO: Use a filter regex?
    def __init__(self, config: Union[dict, str, None]):
        """
        config: A configuration override. Used primarily for ad hoc testing.
            It's preferable to specify a yaml configuration file usually.
        """
        self.conf: Config = Config.load(config)

    def task(self, instance: str, schema: str) -> SchemaImportTask:
        conf = self.conf.get_schema(instance, schema)
        return SchemaImportTask(conf)

    def tasks(self):
        """A generator containing all configured tasks."""
        for instance, instance_node in self.conf["sources"].items():
            for schema in instance_node["schemas"]:
                try:
                    task = self.task(instance, schema)
                except ValueError:
                    logger.warning(f'Unable to create RDS Snapshot inflow task for {instance}.{schema}')
                else:
                    yield self.task(instance, schema)

    def run(self):
        for task in self.tasks():
            task.run()
