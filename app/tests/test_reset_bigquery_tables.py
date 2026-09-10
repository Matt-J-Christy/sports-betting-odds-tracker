import pytest

from scripts.reset_bigquery_tables import reset_tables


def test_reset_rejects_unexpected_dataset():
    with pytest.raises(ValueError, match="unexpected dataset"):
        reset_tables("other-project", "nfl_data", "other-project.nfl_data")


def test_reset_requires_exact_confirmation():
    with pytest.raises(ValueError, match="destructive reset"):
        reset_tables("odds-tracker-402301", "nfl_data", "yes")
