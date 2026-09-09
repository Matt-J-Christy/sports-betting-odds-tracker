"""Unit tests for Power 4 conference filtering.

Covers:
    - is_power_4_team() alias resolution (case-insensitive, various spellings)
    - is_p4_ncaaf_game() logic for retain/drop decisions
    - Integration with OddsQuery._normalize_event_rows():
        * P4 vs P4 game → retained
        * P4 vs non-P4 game → retained
        * non-P4 vs non-P4 game → dropped
        * NFL game → always retained (no filtering)
"""

from __future__ import annotations

import pytest

from app.config.conferences import is_p4_ncaaf_game, is_power_4_team
from app.query_betting_api import OddsQuery

# ─────────────────────────────────────────────────────────────────────────────
# Fixtures helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_event(
    sport: str,
    home_team: str,
    away_team: str,
    game_id: str = "test_game_001",
) -> dict:
    """Build a minimal Odds API event dict with a single FanDuel spread outcome."""
    return {
        "id": game_id,
        "sport_key": sport,
        "home_team": home_team,
        "away_team": away_team,
        "commence_time": "2026-09-13T18:00:00Z",
        "bookmakers": [
            {
                "key": "fanduel",
                "markets": [
                    {
                        "key": "spreads",
                        "outcomes": [
                            {"name": home_team, "price": -110, "point": -3.5},
                            {"name": away_team, "price": -110, "point": 3.5},
                        ],
                    }
                ],
            }
        ],
    }


_QUERY = OddsQuery(api_key="test-key", gcp_service_accnt=None)

# ─────────────────────────────────────────────────────────────────────────────
# is_power_4_team
# ─────────────────────────────────────────────────────────────────────────────

class TestIsPower4Team:
    """Verify alias resolution for known and unknown team names."""

    @pytest.mark.parametrize("name", [
        "Alabama",
        "alabama crimson tide",
        "GEORGIA",
        "Ohio State Buckeyes",
        "ohio state",
        "Clemson Tigers",
        "BYU",
        "Brigham Young",
        "Pitt Panthers",
        "Pittsburgh",
        "cal",
        "California Golden Bears",
        "USC Trojans",
        "Southern California",
        "Miami (FL)",
        "Miami Hurricanes",
        "Notre Dame",
        "notre dame fighting irish",
        "Texas A&M Aggies",
        "Texas A&M",
        "NC State",
        "North Carolina State",
        "Ole Miss",
        "Mississippi Rebels",
        "UCF Knights",
    ])
    def test_known_power4_team_returns_true(self, name: str):
        assert is_power_4_team(name) is True, f"Expected {name!r} to be Power 4"

    @pytest.mark.parametrize("name", [
        "Akron Zips",
        "akron",
        "Buffalo Bulls",
        "Fresno State",
        "San Jose State",
        "Eastern Michigan",
        "Ball State Eagles",
        "Bowling Green",
        "",
        "   ",
    ])
    def test_non_power4_team_returns_false(self, name: str):
        assert is_power_4_team(name) is False, f"Expected {name!r} to be non-Power 4"


# ─────────────────────────────────────────────────────────────────────────────
# is_p4_ncaaf_game
# ─────────────────────────────────────────────────────────────────────────────

class TestIsp4NcaafGame:
    def test_both_power4_returns_true(self):
        assert is_p4_ncaaf_game("Georgia Bulldogs", "Alabama Crimson Tide") is True

    def test_home_p4_away_not_returns_true(self):
        assert is_p4_ncaaf_game("Ohio State Buckeyes", "Akron Zips") is True

    def test_home_not_away_p4_returns_true(self):
        assert is_p4_ncaaf_game("Akron Zips", "Clemson Tigers") is True

    def test_neither_p4_returns_false(self):
        assert is_p4_ncaaf_game("Buffalo Bulls", "Akron Zips") is False

    def test_notre_dame_always_included(self):
        assert is_p4_ncaaf_game("Notre Dame Fighting Irish", "Navy Midshipmen") is True


# ─────────────────────────────────────────────────────────────────────────────
# OddsQuery._normalize_event_rows integration
# ─────────────────────────────────────────────────────────────────────────────

class TestOddsQueryP4Filter:
    """Verify the filter gate inside _normalize_event_rows."""

    def test_ncaaf_p4_vs_p4_retained(self):
        event = _make_event("americanfootball_ncaaf", "Georgia Bulldogs", "Alabama Crimson Tide")
        df = _QUERY._normalize_event_rows([event])
        assert not df.empty, "P4 vs P4 NCAAF game must be retained"
        assert df["home_team"].iloc[0] == "Georgia Bulldogs"

    def test_ncaaf_p4_vs_nonp4_retained(self):
        event = _make_event("americanfootball_ncaaf", "Ohio State Buckeyes", "Akron Zips")
        df = _QUERY._normalize_event_rows([event])
        assert not df.empty, "P4 vs non-P4 NCAAF game must be retained"

    def test_ncaaf_nonp4_vs_p4_retained(self):
        event = _make_event("americanfootball_ncaaf", "Akron Zips", "Clemson Tigers")
        df = _QUERY._normalize_event_rows([event])
        assert not df.empty, "Non-P4 vs P4 NCAAF game must be retained"

    def test_ncaaf_nonp4_vs_nonp4_dropped(self):
        event = _make_event("americanfootball_ncaaf", "Buffalo Bulls", "Akron Zips")
        df = _QUERY._normalize_event_rows([event])
        assert df.empty, "Non-P4 vs non-P4 NCAAF game must be dropped"

    def test_nfl_game_always_retained(self):
        """NFL events must bypass the P4 filter entirely."""
        event = _make_event(
            "americanfootball_nfl",
            "Kansas City Chiefs",
            "Buffalo Bills",
        )
        df = _QUERY._normalize_event_rows([event])
        assert not df.empty, "NFL game must always be retained"

    def test_mixed_batch_filters_correctly(self):
        """A batch containing P4, non-P4, and NFL events is filtered correctly."""
        events = [
            _make_event("americanfootball_ncaaf", "Georgia Bulldogs", "Akron Zips", "keep_1"),
            _make_event("americanfootball_ncaaf", "Buffalo Bulls", "Akron Zips", "drop_1"),
            _make_event("americanfootball_nfl", "Kansas City Chiefs", "Buffalo Bills", "keep_nfl"),
            _make_event("americanfootball_ncaaf", "Notre Dame Fighting Irish", "Navy Midshipmen", "keep_nd"),
        ]
        df = _QUERY._normalize_event_rows(events)
        retained_ids = df["game_id"].unique().tolist()
        assert "keep_1" in retained_ids
        assert "keep_nfl" in retained_ids
        assert "keep_nd" in retained_ids
        assert "drop_1" not in retained_ids

    def test_returned_dataframe_schema_unchanged(self):
        """Filtering must not alter the output DataFrame schema."""
        event = _make_event("americanfootball_ncaaf", "Ohio State Buckeyes", "Akron Zips")
        df = _QUERY._normalize_event_rows([event])
        expected_cols = [
            "snapshot_time", "game_id", "sport", "away_team", "home_team",
            "commence_time", "market_type", "team_or_outcome", "price", "market_point",
        ]
        assert list(df.columns) == expected_cols

