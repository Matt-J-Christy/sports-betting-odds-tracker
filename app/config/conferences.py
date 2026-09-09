"""Power 4 Conference team registry and filtering helpers for NCAA Football.

The Odds API returns college football team names in a variety of formats.
This module maintains a normalized alias map so that every known variant of
a school name resolves to its canonical Power 4 membership, regardless of
how The Odds API or a specific bookmaker happens to spell it.

Conferences as of the 2026 season:
    SEC  (16): Alabama, Arkansas, Auburn, Florida, Georgia, Kentucky, LSU,
               Mississippi State, Missouri, Oklahoma, Ole Miss (Mississippi),
               South Carolina, Tennessee, Texas, Texas A&M, Vanderbilt
    Big Ten (18): Illinois, Indiana, Iowa, Maryland, Michigan, Michigan State,
               Minnesota, Nebraska, Northwestern, Ohio State, Oregon, Penn State,
               Purdue, Rutgers, UCLA, USC, Washington, Wisconsin
    Big 12 (16): Arizona, Arizona State, Baylor, BYU, Cincinnati, Colorado,
               Houston, Iowa State, Kansas, Kansas State, Oklahoma State, TCU,
               Texas Tech, UCF, Utah, West Virginia
    ACC  (17): Boston College, Cal, Clemson, Duke, Florida State, Georgia Tech,
               Louisville, Miami FL, NC State, North Carolina, Pittsburgh, SMU,
               Stanford, Syracuse, Virginia, Virginia Tech, Wake Forest
    Independent (included): Notre Dame
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Canonical names and their aliases
# Each key is a normalized lowercase alias; the value marks it as P4.
# The alias table intentionally casts a wide net for The Odds API naming
# variations (full university name, nickname-only, abbreviation, etc.).
# ---------------------------------------------------------------------------

_P4_ALIASES: frozenset[str] = frozenset(
    [
        # ── SEC ──────────────────────────────────────────────────────────────
        "alabama", "alabama crimson tide",
        "arkansas", "arkansas razorbacks",
        "auburn", "auburn tigers",
        "florida", "florida gators",
        "georgia", "georgia bulldogs",
        "kentucky", "kentucky wildcats",
        "lsu", "lsu tigers", "louisiana state", "louisiana state tigers",
        "mississippi state", "mississippi state bulldogs",
        "missouri", "missouri tigers",
        "oklahoma", "oklahoma sooners",
        "ole miss", "mississippi", "mississippi rebels",
        "south carolina", "south carolina gamecocks",
        "tennessee", "tennessee volunteers", "tennessee vols",
        "texas", "texas longhorns",
        "texas a&m", "texas a&m aggies", "texas am", "texas am aggies",
        "vanderbilt", "vanderbilt commodores",

        # ── Big Ten ───────────────────────────────────────────────────────────
        "illinois", "illinois fighting illini",
        "indiana", "indiana hoosiers",
        "iowa", "iowa hawkeyes",
        "maryland", "maryland terrapins", "maryland terps",
        "michigan", "michigan wolverines",
        "michigan state", "michigan state spartans",
        "minnesota", "minnesota golden gophers",
        "nebraska", "nebraska cornhuskers",
        "northwestern", "northwestern wildcats",
        "ohio state", "ohio state buckeyes",
        "oregon", "oregon ducks",
        "penn state", "penn state nittany lions",
        "purdue", "purdue boilermakers",
        "rutgers", "rutgers scarlet knights",
        "ucla", "ucla bruins",
        "usc", "usc trojans", "southern california", "southern california trojans",
        "washington", "washington huskies",
        "wisconsin", "wisconsin badgers",

        # ── Big 12 ───────────────────────────────────────────────────────────
        "arizona", "arizona wildcats",
        "arizona state", "arizona state sun devils",
        "baylor", "baylor bears",
        "byu", "brigham young", "brigham young cougars", "byu cougars",
        "cincinnati", "cincinnati bearcats",
        "colorado", "colorado buffaloes",
        "houston", "houston cougars",
        "iowa state", "iowa state cyclones",
        "kansas", "kansas jayhawks",
        "kansas state", "kansas state wildcats",
        "oklahoma state", "oklahoma state cowboys",
        "tcu", "tcu horned frogs", "texas christian", "texas christian horned frogs",
        "texas tech", "texas tech red raiders",
        "ucf", "ucf knights", "central florida", "central florida knights",
        "utah", "utah utes",
        "west virginia", "west virginia mountaineers",

        # ── ACC ──────────────────────────────────────────────────────────────
        "boston college", "boston college eagles",
        "cal", "california", "california golden bears", "cal bears",
        "clemson", "clemson tigers",
        "duke", "duke blue devils",
        "florida state", "florida state seminoles", "fsu",
        "georgia tech", "georgia tech yellow jackets",
        "louisville", "louisville cardinals",
        "miami", "miami hurricanes", "miami fl", "miami (fl)",
        "nc state", "nc state wolfpack", "north carolina state",
        "north carolina", "north carolina tar heels", "unc",
        "pittsburgh", "pittsburgh panthers", "pitt", "pitt panthers",
        "smu", "smu mustangs", "southern methodist", "southern methodist mustangs",
        "stanford", "stanford cardinal",
        "syracuse", "syracuse orange",
        "virginia", "virginia cavaliers",
        "virginia tech", "virginia tech hokies",
        "wake forest", "wake forest demon deacons",

        # ── Notable Independent (always include) ─────────────────────────────
        "notre dame", "notre dame fighting irish",
    ]
)


def is_power_4_team(team_name: str) -> bool:
    """Return True if *team_name* maps to a Power 4 (or Notre Dame) program.

    The lookup is case-insensitive and strips leading/trailing whitespace.
    Unknown schools return False.

    Examples:
        >>> is_power_4_team("Ohio State Buckeyes")
        True
        >>> is_power_4_team("Akron Zips")
        False
        >>> is_power_4_team("Pitt Panthers")
        True
        >>> is_power_4_team("Miami (FL)")
        True
    """
    if not team_name:
        return False
    return team_name.strip().lower() in _P4_ALIASES


def is_p4_ncaaf_game(home_team: str, away_team: str) -> bool:
    """Return True if at least one participant is a Power 4 program.

    Use this to decide whether to keep an NCAA football event coming from
    The Odds API.  Games where neither side is Power 4 (e.g., MAC vs Sun Belt)
    are ignored for the current data collection scope.

    Args:
        home_team: Value of ``event["home_team"]`` from The Odds API response.
        away_team: Value of ``event["away_team"]`` from The Odds API response.

    Returns:
        True if at least one team is Power 4 (or Notre Dame), False otherwise.
    """
    return is_power_4_team(home_team) or is_power_4_team(away_team)

