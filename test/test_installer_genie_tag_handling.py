from types import SimpleNamespace

import pytest

from dbdemos.exceptions.dbdemos_exception import SQLQueryException
from dbdemos.installer_genie import InstallerGenie


class _SqlExecutorStub:
    def __init__(self, side_effect=None):
        self.queries = []
        self.side_effect = side_effect

    def execute_query(self, ws, query, warehouse_id=None, debug=False):
        self.queries.append(query)
        if self.side_effect:
            raise self.side_effect


def _new_installer_genie(executor):
    fake_installer = SimpleNamespace(db=None)
    genie_installer = InstallerGenie(fake_installer)
    genie_installer.sql_query_executor = executor
    return genie_installer


def test_run_sql_queries_sanitizes_reserved_system_certified_tag_key():
    executor = _SqlExecutorStub()
    genie_installer = _new_installer_genie(executor)
    demo_conf = SimpleNamespace(
        sql_queries=[["ALTER TABLE my_table SET TAGS ('system.Certified' = 'true')"]]
    )

    genie_installer.run_sql_queries(object(), demo_conf, warehouse_id="wh", debug=False)

    assert executor.queries == [
        "ALTER TABLE my_table SET TAGS ('system_Certified' = 'true')"
    ]


def test_run_sql_queries_ignores_system_certified_tag_validation_error():
    executor = _SqlExecutorStub(
        side_effect=SQLQueryException(
            "Query execution failed: Tag key 'system.Certified' is invalid"
        )
    )
    genie_installer = _new_installer_genie(executor)
    demo_conf = SimpleNamespace(
        sql_queries=[["ALTER TABLE my_table SET TAGS ('system.Certified' = 'true')"]]
    )

    genie_installer.run_sql_queries(object(), demo_conf, warehouse_id="wh", debug=False)


def test_run_sql_queries_keeps_raising_non_tag_errors():
    executor = _SqlExecutorStub(side_effect=SQLQueryException("some unrelated SQL error"))
    genie_installer = _new_installer_genie(executor)
    demo_conf = SimpleNamespace(sql_queries=[["SELECT 1"]])

    with pytest.raises(SQLQueryException):
        genie_installer.run_sql_queries(object(), demo_conf, warehouse_id="wh", debug=False)
