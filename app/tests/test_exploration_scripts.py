from scripts.explore_api_data import _load_fixture
from scripts.preview_bigquery_tables import TABLES, _create_statement


def test_fixture_explorer_matches_table_shapes():
    assert len(_load_fixture("odds")) == 4
    assert len(_load_fixture("metadata").columns) == len(TABLES["metadata"][1])
    assert len(_load_fixture("scores").columns) == len(TABLES["scores"][1])


def test_table_preview_generates_schema_sql():
    statement = _create_statement("project.dataset.table", TABLES["odds"][1])

    assert "CREATE TABLE IF NOT EXISTS `project.dataset.table`" in statement
    assert "`market_point` FLOAT64" in statement