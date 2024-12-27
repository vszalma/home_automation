import pytest
from unittest.mock import patch
from backup_master import coordinate_backup_process

@patch("robocopy_helper._run_robocopy")
@patch("backup_master._calculate_enough_space_available")
def test_coordinate_backup_process(mock_calculate_enough_space_available, mock_run_robocopy):

    mock_run_robocopy.return_value = True

    mock_calculate_enough_space_available.return_value = False
    result = coordinate_backup_process("F:\\_bu-2024-11-16", "G:\\Backups", True)
    assert result == False


# pytest --capture=no --log-cli-level=INFO tests\test_automation_master.py