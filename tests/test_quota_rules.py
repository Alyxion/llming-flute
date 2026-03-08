"""Unit tests for flute.quota_rules."""

from flute.quota_rules import get_quota_for_type, parse_quota_rules


class TestParseQuotaRules:
    def test_single_wildcard(self):
        rules = parse_quota_rules("*:600/3600")
        assert rules == {"*": (600, 3600)}

    def test_multiple_types(self):
        rules = parse_quota_rules("*:600/3600 image:120/1800")
        assert rules == {"*": (600, 3600), "image": (120, 1800)}

    def test_no_wildcard(self):
        rules = parse_quota_rules("python:300/1800 image:60/900")
        assert rules == {"python": (300, 1800), "image": (60, 900)}

    def test_empty_string(self):
        assert parse_quota_rules("") == {}

    def test_whitespace_only(self):
        assert parse_quota_rules("   ") == {}

    def test_invalid_entry_no_colon(self):
        rules = parse_quota_rules("invalid *:600/3600")
        assert rules == {"*": (600, 3600)}

    def test_invalid_entry_no_slash(self):
        rules = parse_quota_rules("bad:noslash *:600/3600")
        assert rules == {"*": (600, 3600)}

    def test_extra_whitespace(self):
        rules = parse_quota_rules("  *:600/3600   image:120/1800  ")
        assert rules == {"*": (600, 3600), "image": (120, 1800)}


class TestGetQuotaForType:
    def test_exact_match(self):
        rules = {"python": (300, 1800), "image": (120, 900)}
        assert get_quota_for_type(rules, "python") == (300, 1800)
        assert get_quota_for_type(rules, "image") == (120, 900)

    def test_wildcard_fallback(self):
        rules = {"*": (600, 3600), "image": (120, 1800)}
        assert get_quota_for_type(rules, "python") == (600, 3600)
        assert get_quota_for_type(rules, "image") == (120, 1800)

    def test_default_fallback(self):
        rules = {"image": (120, 1800)}
        assert get_quota_for_type(rules, "python") == (600, 3600)

    def test_custom_defaults(self):
        rules = {}
        assert get_quota_for_type(rules, "python", default_limit=100, default_interval=900) == (
            100, 900,
        )

    def test_empty_rules(self):
        assert get_quota_for_type({}, "anything") == (600, 3600)
