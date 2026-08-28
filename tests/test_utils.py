"""Tests for utility functions."""

from jira_ingest.utils import clean_whitespace, hash_pii, safe_bool, safe_int, safe_str


class TestHashPii:
    def test_consistent(self) -> None:
        assert hash_pii("Alice", "alice-key") == hash_pii("Alice", "alice-key")

    def test_different_inputs(self) -> None:
        assert hash_pii("Alice") != hash_pii("Bob")

    def test_returns_64_char_hex(self) -> None:
        result = hash_pii("any input")
        assert len(result) == 64
        assert all(c in "0123456789abcdef" for c in result)

    def test_field_boundary_does_not_collide(self) -> None:
        """Regression test for #6: plain concatenation let different field
        splits hash identically, e.g. ("John", "Smith1") vs ("JohnSmith", "1")."""
        assert hash_pii("John", "Smith1") != hash_pii("JohnSmith", "1")
        assert hash_pii("JohnS", "mith1") != hash_pii("JohnSmith", "1")


class TestSafeInt:
    def test_valid_int_string(self) -> None:
        assert safe_int("42") == 42

    def test_none_returns_none(self) -> None:
        assert safe_int(None) is None

    def test_empty_string_returns_none(self) -> None:
        assert safe_int("") is None

    def test_non_numeric_returns_none(self) -> None:
        assert safe_int("abc") is None

    def test_already_int(self) -> None:
        assert safe_int(7) == 7


class TestSafeStr:
    def test_none_returns_empty(self) -> None:
        assert safe_str(None) == ""

    def test_int_to_str(self) -> None:
        assert safe_str(42) == "42"

    def test_string_passthrough(self) -> None:
        assert safe_str("hello") == "hello"


class TestSafeBool:
    def test_true_variants(self) -> None:
        for v in ("true", "1", "yes", "True", "YES"):
            assert safe_bool(v) is True

    def test_false_variants(self) -> None:
        for v in ("false", "0", "no", "False", "NO"):
            assert safe_bool(v) is False

    def test_bool_passthrough(self) -> None:
        assert safe_bool(True) is True
        assert safe_bool(False) is False

    def test_unknown_returns_none(self) -> None:
        assert safe_bool("maybe") is None


class TestCleanWhitespace:
    def test_strips_string(self) -> None:
        assert clean_whitespace("  hello  ") == "hello"

    def test_recurses_into_dict(self) -> None:
        result = clean_whitespace({"key": "  value  "})
        assert result == {"key": "value"}

    def test_recurses_into_list(self) -> None:
        result = clean_whitespace(["  a  ", "  b  "])
        assert result == ["a", "b"]

    def test_non_string_values_unchanged(self) -> None:
        assert clean_whitespace(42) == 42
        assert clean_whitespace(None) is None
