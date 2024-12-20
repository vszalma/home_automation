import pytest
from unittest.mock import patch, MagicMock
from home_automation_master import _backup_needed
from home_automation_master import coordinate_backup_process
from home_automation_master import _handle_backup_and_validate
import home_automation_master
from robocopy_helper import _run_robocopy
from robocopy_helper import execute_robocopy

@patch("home_automation_master._backup_needed")
@patch("backup._run_robocopy")

def test_execute_backup(mock_run_robocopy):
    mock_run_robocopy.return_value = True

def test_coordinate_backup_process():
    # mock_backup_needed.return_value = True

    #coordinate_backup_process("a", "b")
    result = home_automation_master.coordinate_backup_process("F:\\_bu-2024-11-16", "G:\\Backups", False)
    assert result == False