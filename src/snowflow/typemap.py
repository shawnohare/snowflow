from typing import Optional


class KeywordMap:
    """Will map a keyword used by Snowflake to the quoted version."""

    description = {
        0: "Cannot be used as an identifier in a SHOW command (e.g. ‘SHOW … IN <identifier>’).",
        1: "Cannot be used as column reference in a scalar expression.",
        2: "Reserved by ANSI.",
        3: "Reserved by Snowflake and others.",
        4: "Cannot be used as a column name in CREATE TABLE DDL.",
        5: "Cannot be used as table name or alias in a FROM clause.",
        6: "Cannot be used as column name (reserved by ANSI).",
        7: "Reserved by Snowflake.",
    }

    # word, description.
    keywords = {
        "account": 0,
        "all": 2,
        "alter": 2,
        "and": 2,
        "any": 2,
        "as": 2,
        "between": 2,
        "by": 2,
        "case": 1,
        "cast": 1,
        "check": 2,
        "column": 2,
        "connect": 2,
        "connection": 0,
        "constraint": 4,
        "create": 2,
        "cross": 5,
        "current": 2,
        "current_date": 6,
        "current_time": 6,
        "current_timestamp": 6,
        "current_user": 6,
        "database": 0,
        "delete": 2,
        "distinct": 2,
        "drop": 2,
        "else": 2,
        "exists": 2,
        "false": 1,
        "following": 2,
        "for": 2,
        "from": 2,
        "full": 5,
        "grant": 2,
        "group": 2,
        "gscluster": 0,
        "having": 2,
        "ilike": 7,
        "in": 2,
        "increment": 3,
        "inner": 5,
        "insert": 2,
        "intersect": 2,
        "into": 2,
        "is": 2,
        "issue": 0,
        "join": 5,
        "lateral": 5,
        "left": 5,
        "like": 2,
        "localtime": 6,
        "localtimestamp": 6,
        "minus": 3,
        "natural": 5,
        "not": 2,
        "null": 2,
        "of": 2,
        "on": 2,
        "or": 2,
        "order": 2,
        "organization": 0,
        "qualify": 7,
        "regexp": 7,
        "revoke": 2,
        "right": 5,
        "rlike": 7,
        "row": 2,
        "rows": 2,
        "sample": 2,
        "schema": 0,
        "select": 2,
        "set": 2,
        "some": 7,
        "start": 2,
        "table": 2,
        "tablesample": 2,
        "then": 2,
        "to": 2,
        "trigger": 2,
        "true": 1,
        "try_cast": 1,
        "union": 2,
        "unique": 2,
        "update": 2,
        "using": 5,
        "values": 2,
        "view": 0,
        "when": 1,
        "whenever": 2,
        "where": 2,
        "with": 2,
    }


    @classmethod
    def map(cls, word: str) -> str:
        """Quote a word if it is a keyword."""
        # TODO: More intelligent routing based on the description.
        # Some things probably do not need to be quoted
        if word in cls.keywords:
            word = f'"{word}"'
        return word


class TypeMap:
    """A mapping interface to convert types encountered in data sources
    to the analogous Snowflake types.

    ClassVars:
        to_snowflake: A mapping from external source types to Snowflake types.

    """

    # TODO: Take into account precision information.

    # mapping
    # - Snowflake has int, integer, bigint, smallint, tinyint, byteint
    #   as synonyms for Number(38, 0) for conversion purposes.
    # - double, double precision and real are synonyms for float
    # - text, string are synonyms for varchar
    # - json is either "variant" or, if a key, value map "object"
    to_snowflake: dict = {
        "defaults": {
            "mediumint": "int",
            "tinytext": "text",
            "mediumtext": "text",
            "longtext": "text",
            "bit": "boolean",
            "json": "variant",
        },
        "mysql": {},
    }

    @classmethod
    def mysql_to_snowflake(cls, name: str, meta: Optional[dict]) -> str:
        """Map a MySQL type to a Snowflake type."""
        name = name.replace("unsigned", "").strip()
        mapping = cls.to_snowflake["mysql"]
        tar_type = mapping.get(name)
        if tar_type is None:
            tar_type = cls.to_snowflake["defaults"].get(name, name)
        return tar_type

    @classmethod
    def map(
        cls, name: str, src: str, tar: str = "snowflake", meta: Optional[dict] = None
    ) -> str:
        """Map the input type from the given source (src) to the target (tar)
        output, e.g., map a MySQL type to a Snowflake type.

        Args:
            val: A type name, e.g., "mediumint".
            src: Source system of the type, e.g., "mysql".
            tar: Target system into which the type is to be mapped, e.g., "snowflake".
            meta: Type metadata such as precision, encoding, etc.

        Inputs not explicitly mapped have the identity function applied, i.e.,
        are passed through.
        """
        meth = getattr(cls, f"{src}_to_{tar}")
        return meth(name.strip().lower(), meta)
