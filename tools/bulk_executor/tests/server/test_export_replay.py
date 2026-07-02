"""Test for issue #95: export-replay and export-unplay verbs.

export-replay plays incremental export events forward (apply changes).
export-unplay plays them backward (undo changes).

For unplay:
- INSERT (New Image, no Old Image) -> DELETE the item
- DELETE (Old Image, no New Image) -> PUT the Old Image back
- MODIFY (Old Image + New Image) -> PUT the Old Image back

For replay:
- INSERT -> PUT the New Image
- DELETE -> DELETE the item
- MODIFY -> PUT the New Image
"""

from unittest.mock import MagicMock, call, patch

import pytest


class TestExportUnplay:
    """export_unplay reverses incremental export events."""

    @pytest.fixture(autouse=True)
    def import_module(self):
        try:
            from python_modules import export_unplay
            self.module = export_unplay
        except (ImportError, ModuleNotFoundError):
            # Also try revert_export which is the closest existing module
            try:
                from python_modules import revert_export
                # The module exists but might not have the unplay logic
                if hasattr(revert_export, 'undo_change'):
                    self.module = revert_export
                else:
                    pytest.fail(
                        "Neither python_modules.export_unplay exists, nor "
                        "revert_export has an undo_change function"
                    )
            except (ImportError, ModuleNotFoundError):
                pytest.fail("python_modules.export_unplay does not exist")

    def test_undo_insert_produces_delete(self):
        """An INSERT record (NewImage only) should produce a DELETE."""
        record = {
            'eventName': 'INSERT',
            'dynamodb': {
                'NewImage': {'pk': {'S': 'abc'}, 'data': {'S': 'hello'}},
                'Keys': {'pk': {'S': 'abc'}},
            }
        }
        action = self.module.undo_change(record)
        assert action['type'] == 'DELETE'
        assert action['key'] == {'pk': {'S': 'abc'}}

    def test_undo_delete_produces_put(self):
        """A DELETE record (OldImage only) should produce a PUT of the old item."""
        record = {
            'eventName': 'REMOVE',
            'dynamodb': {
                'OldImage': {'pk': {'S': 'xyz'}, 'data': {'S': 'was-here'}},
                'Keys': {'pk': {'S': 'xyz'}},
            }
        }
        action = self.module.undo_change(record)
        assert action['type'] == 'PUT'
        assert action['item'] == {'pk': {'S': 'xyz'}, 'data': {'S': 'was-here'}}

    def test_undo_modify_puts_old_image(self):
        """A MODIFY record should PUT the OldImage back."""
        record = {
            'eventName': 'MODIFY',
            'dynamodb': {
                'OldImage': {'pk': {'S': 'mod'}, 'val': {'N': '1'}},
                'NewImage': {'pk': {'S': 'mod'}, 'val': {'N': '2'}},
                'Keys': {'pk': {'S': 'mod'}},
            }
        }
        action = self.module.undo_change(record)
        assert action['type'] == 'PUT'
        assert action['item'] == {'pk': {'S': 'mod'}, 'val': {'N': '1'}}


class TestExportReplay:
    """export_replay applies incremental export events forward."""

    @pytest.fixture(autouse=True)
    def import_module(self):
        try:
            from python_modules import export_replay
            self.module = export_replay
        except (ImportError, ModuleNotFoundError):
            pytest.fail("python_modules.export_replay does not exist")

    def test_replay_insert_produces_put(self):
        """An INSERT should PUT the NewImage."""
        record = {
            'eventName': 'INSERT',
            'dynamodb': {
                'NewImage': {'pk': {'S': 'new'}, 'data': {'S': 'fresh'}},
                'Keys': {'pk': {'S': 'new'}},
            }
        }
        action = self.module.replay_change(record)
        assert action['type'] == 'PUT'
        assert action['item'] == {'pk': {'S': 'new'}, 'data': {'S': 'fresh'}}

    def test_replay_delete_produces_delete(self):
        """A DELETE/REMOVE should DELETE the item."""
        record = {
            'eventName': 'REMOVE',
            'dynamodb': {
                'OldImage': {'pk': {'S': 'gone'}, 'data': {'S': 'bye'}},
                'Keys': {'pk': {'S': 'gone'}},
            }
        }
        action = self.module.replay_change(record)
        assert action['type'] == 'DELETE'
        assert action['key'] == {'pk': {'S': 'gone'}}

    def test_replay_modify_puts_new_image(self):
        """A MODIFY should PUT the NewImage."""
        record = {
            'eventName': 'MODIFY',
            'dynamodb': {
                'OldImage': {'pk': {'S': 'chg'}, 'val': {'N': '1'}},
                'NewImage': {'pk': {'S': 'chg'}, 'val': {'N': '2'}},
                'Keys': {'pk': {'S': 'chg'}},
            }
        }
        action = self.module.replay_change(record)
        assert action['type'] == 'PUT'
        assert action['item'] == {'pk': {'S': 'chg'}, 'val': {'N': '2'}}
