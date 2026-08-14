"""Unit tests for the client scanfind command module (client/src/python_modules/scanfind.py).

Named test_scanfind_client.py (not test_scanfind.py) to avoid a basename collision
with the server-side tests/server/test_scanfind.py under pytest's rootdir import mode.

Covers:
- json_type: argparse type for the two expression parameters
- positive_int: argparse type for --limit and --segments
- validate_expression_values: --expression-values must be DynamoDB JSON, because
  scanfind talks to the low-level DynamoDB API (this is where it diverges from
  scancount, which takes plain JSON)
- run(): argument wiring, the filter-expression substitution cross-checks, index
  forwarding to table validation, and the fact that a bad value is rejected
  client-side before a Glue job is launched
"""

import argparse
import importlib.util
import os
from unittest.mock import MagicMock, patch

import pytest

CLIENT_SRC = os.path.join(os.path.dirname(__file__), '..', '..', 'client', 'src')


def _import_client_scanfind():
    """Import client/src/python_modules/scanfind.py without colliding with the
    server's python_modules.scanfind package."""
    path = os.path.join(CLIENT_SRC, 'python_modules', 'scanfind.py')
    spec = importlib.util.spec_from_file_location('client_scanfind', os.path.abspath(path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _argv(*args):
    """Build a `bulk scanfind ...` argv."""
    return ['bulk', 'scanfind', *args]


class TestJsonType:
    """The expression parameters are passed through as strings, but must parse."""

    def test_valid_json_returned_unchanged(self):
        scanfind = _import_client_scanfind()
        raw = '{":v": {"S": "x"}}'
        assert scanfind.json_type(raw) == raw

    def test_invalid_json_raises(self):
        scanfind = _import_client_scanfind()
        with pytest.raises(argparse.ArgumentTypeError, match="invalid JSON"):
            scanfind.json_type('{not json')

    def test_error_includes_the_offending_string(self):
        """The message quotes the input, because a shell-quoting mistake is the
        usual cause and is invisible otherwise."""
        scanfind = _import_client_scanfind()
        with pytest.raises(argparse.ArgumentTypeError, match="oops"):
            scanfind.json_type('oops')


class TestPositiveInt:
    """positive_int guards --limit and --segments."""

    def test_valid_positive_returns_int(self):
        scanfind = _import_client_scanfind()
        assert scanfind.positive_int('100') == 100

    def test_non_integer_raises(self):
        scanfind = _import_client_scanfind()
        with pytest.raises(argparse.ArgumentTypeError, match="must be an integer"):
            scanfind.positive_int('abc')

    def test_zero_raises(self):
        scanfind = _import_client_scanfind()
        with pytest.raises(argparse.ArgumentTypeError, match="must be a positive integer"):
            scanfind.positive_int('0')

    def test_negative_raises(self):
        scanfind = _import_client_scanfind()
        with pytest.raises(argparse.ArgumentTypeError, match="must be a positive integer"):
            scanfind.positive_int('-5')

    def test_float_string_raises(self):
        scanfind = _import_client_scanfind()
        with pytest.raises(argparse.ArgumentTypeError, match="must be an integer"):
            scanfind.positive_int('2.5')


class TestValidateExpressionValues:
    """--expression-values must already be DynamoDB JSON.

    Catching a plain-JSON value here turns a Glue-job ValidationException that
    costs minutes into an immediate message.
    """

    def _parser(self):
        parser = MagicMock()
        parser.error = MagicMock(side_effect=SystemExit(2))
        return parser

    def test_wire_format_values_accepted(self):
        scanfind = _import_client_scanfind()
        parser = self._parser()

        scanfind.validate_expression_values(
            parser, '{":s": {"S": "x"}, ":n": {"N": "5"}, ":b": {"BOOL": true}}')

        parser.error.assert_not_called()

    def test_every_attribute_value_type_accepted(self):
        """All ten AttributeValue shapes are legal, including the collections."""
        scanfind = _import_client_scanfind()
        parser = self._parser()

        scanfind.validate_expression_values(parser, (
            '{":s": {"S": "x"}, ":n": {"N": "1"}, ":b": {"B": "AAE="}, '
            '":ss": {"SS": ["a"]}, ":ns": {"NS": ["1"]}, ":bs": {"BS": ["AAE="]}, '
            '":m": {"M": {"k": {"S": "v"}}}, ":l": {"L": [{"S": "v"}]}, '
            '":nul": {"NULL": true}, ":bool": {"BOOL": false}}'))

        parser.error.assert_not_called()

    def test_plain_json_number_rejected(self):
        """'{":n": 5}' is the mistake this check exists for."""
        scanfind = _import_client_scanfind()
        parser = self._parser()

        with pytest.raises(SystemExit):
            scanfind.validate_expression_values(parser, '{":n": 5}')

        message = parser.error.call_args.args[0]
        assert ':n' in message and 'not DynamoDB JSON' in message

    def test_plain_json_string_rejected(self):
        """The shape scancount takes — the most likely thing a user copies over."""
        scanfind = _import_client_scanfind()
        parser = self._parser()

        with pytest.raises(SystemExit):
            scanfind.validate_expression_values(parser, '{":ts": "2025-01-01"}')

        assert 'not DynamoDB JSON' in parser.error.call_args.args[0]

    def test_error_shows_the_wire_form_to_use(self):
        """The message has to say what to write instead, not just that it's wrong."""
        scanfind = _import_client_scanfind()
        parser = self._parser()

        with pytest.raises(SystemExit):
            scanfind.validate_expression_values(parser, '{":n": 5}')

        message = parser.error.call_args.args[0]
        assert '{"N": "5"}' in message
        assert 'scancount' in message, "names the verb whose syntax differs"

    def test_unknown_type_key_rejected(self):
        scanfind = _import_client_scanfind()
        parser = self._parser()

        with pytest.raises(SystemExit):
            scanfind.validate_expression_values(parser, '{":v": {"STRING": "x"}}')

    def test_multi_key_object_rejected(self):
        """An AttributeValue carries exactly one type key."""
        scanfind = _import_client_scanfind()
        parser = self._parser()

        with pytest.raises(SystemExit):
            scanfind.validate_expression_values(parser, '{":v": {"S": "x", "N": "1"}}')

    def test_json_array_rejected(self):
        scanfind = _import_client_scanfind()
        parser = self._parser()

        with pytest.raises(SystemExit):
            scanfind.validate_expression_values(parser, '[{"S": "x"}]')

        assert 'must be a JSON object' in parser.error.call_args.args[0]

    def test_empty_object_accepted(self):
        """Nothing to check; the server will reject it if a filter needs values."""
        scanfind = _import_client_scanfind()
        parser = self._parser()

        scanfind.validate_expression_values(parser, '{}')

        parser.error.assert_not_called()


@patch('utils.validate_tables')
class TestRun:
    """run() parses argv, cross-checks the filter expression, and validates the
    table before returning the argument dict that is forwarded to the Glue job."""

    def test_minimal_invocation(self, mock_validate_tables):
        scanfind = _import_client_scanfind()
        with patch('sys.argv', _argv('--table', 'orders')):
            ok, result = scanfind.run(MagicMock())

        assert ok is True
        assert result['table'] == 'orders'
        assert result['verb'] == 'scanfind'
        mock_validate_tables.assert_called_once()

    def test_segments_defaults_to_200(self, mock_validate_tables):
        scanfind = _import_client_scanfind()
        with patch('sys.argv', _argv('--table', 'orders')):
            _, result = scanfind.run(MagicMock())

        assert result['segments'] == 200

    def test_optional_args_absent_when_not_given(self, mock_validate_tables):
        """SUPPRESS keeps unset options out of the dict entirely, so they are
        never forwarded to the Glue job as the string 'None'."""
        scanfind = _import_client_scanfind()
        with patch('sys.argv', _argv('--table', 'orders')):
            _, result = scanfind.run(MagicMock())

        for absent in ('filter_expression', 'expression_names', 'expression_values',
                       'index', 'limit'):
            assert absent not in result

    def test_limit_parsed_as_int(self, mock_validate_tables):
        scanfind = _import_client_scanfind()
        with patch('sys.argv', _argv('--table', 'orders', '--limit', '25')):
            _, result = scanfind.run(MagicMock())

        assert result['limit'] == 25

    def test_filter_expression_with_wire_format_values(self, mock_validate_tables):
        scanfind = _import_client_scanfind()
        argv = _argv('--table', 'audit', '--filter-expression', '#ts > :ts',
                     '--expression-names', '{"#ts": "timestamp"}',
                     '--expression-values', '{":ts": {"S": "2025-01-01"}}')
        with patch('sys.argv', argv):
            _, result = scanfind.run(MagicMock())

        assert result['filter_expression'] == '#ts > :ts'
        assert result['expression_names'] == '{"#ts": "timestamp"}'
        assert result['expression_values'] == '{":ts": {"S": "2025-01-01"}}'

    def test_index_forwarded_to_table_validation(self, mock_validate_tables):
        scanfind = _import_client_scanfind()
        with patch('sys.argv', _argv('--table', 'orders', '--index', 'status-index')):
            _, result = scanfind.run(MagicMock())

        assert result['index'] == 'status-index'
        assert mock_validate_tables.call_args.kwargs['index'] == 'status-index'

    def test_no_index_kwarg_when_scanning_the_base_table(self, mock_validate_tables):
        scanfind = _import_client_scanfind()
        with patch('sys.argv', _argv('--table', 'orders')):
            scanfind.run(MagicMock())

        assert 'index' not in mock_validate_tables.call_args.kwargs

    def test_missing_table_rejected(self, mock_validate_tables):
        scanfind = _import_client_scanfind()
        with patch('sys.argv', _argv()):
            with pytest.raises(SystemExit):
                scanfind.run(MagicMock())

        mock_validate_tables.assert_not_called()

    def test_name_substitution_requires_expression_names(self, mock_validate_tables):
        scanfind = _import_client_scanfind()
        with patch('sys.argv', _argv('--table', 't', '--filter-expression', '#s = :v',
                                     '--expression-values', '{":v": {"S": "x"}}')):
            with pytest.raises(SystemExit):
                scanfind.run(MagicMock())

    def test_value_substitution_requires_expression_values(self, mock_validate_tables):
        scanfind = _import_client_scanfind()
        with patch('sys.argv', _argv('--table', 't', '--filter-expression', 'total > :min')):
            with pytest.raises(SystemExit):
                scanfind.run(MagicMock())

    def test_plain_json_expression_values_rejected_before_launch(self, mock_validate_tables):
        """The wire-format check runs before table validation, so a plain-JSON
        value never reaches a Glue job."""
        scanfind = _import_client_scanfind()
        with patch('sys.argv', _argv('--table', 't', '--filter-expression', 'total > :min',
                                     '--expression-values', '{":min": 100}')):
            with pytest.raises(SystemExit):
                scanfind.run(MagicMock())

        mock_validate_tables.assert_not_called()

    def test_malformed_json_rejected_before_launch(self, mock_validate_tables):
        scanfind = _import_client_scanfind()
        with patch('sys.argv', _argv('--table', 't', '--filter-expression', 'total > :min',
                                     '--expression-values', '{":min": ')):
            with pytest.raises(SystemExit):
                scanfind.run(MagicMock())

        mock_validate_tables.assert_not_called()

    def test_non_positive_limit_rejected_before_launch(self, mock_validate_tables):
        scanfind = _import_client_scanfind()
        with patch('sys.argv', _argv('--table', 'orders', '--limit', '0')):
            with pytest.raises(SystemExit):
                scanfind.run(MagicMock())

        mock_validate_tables.assert_not_called()

    def test_non_positive_segments_rejected_before_launch(self, mock_validate_tables):
        scanfind = _import_client_scanfind()
        with patch('sys.argv', _argv('--table', 'orders', '--segments', '-1')):
            with pytest.raises(SystemExit):
                scanfind.run(MagicMock())

        mock_validate_tables.assert_not_called()


class TestHelpText:
    """The help text is the only place the wire-format requirement is explained,
    so it must actually carry the example."""

    def test_documents_the_dynamodb_json_value_form(self):
        scanfind = _import_client_scanfind()

        assert '{"N": "5"}' in scanfind.help_text
        assert 'DynamoDB JSON' in scanfind.help_text

    def test_notes_the_divergence_from_scancount(self):
        scanfind = _import_client_scanfind()

        assert 'scancount' in scanfind.help_text
