import pytest
from unittest.mock import patch, MagicMock
from backup_master import _backup_needed

@patch("home_automation_master._list_and_sort_directories")
@patch("home_automation_master.collector.collect_file_info")
@patch("home_automation_master.compare.compare_files")
@patch("home_automation_master.home_automation_common.send_email")
def test_backup_needed(
    mock_send_email, mock_compare_files, mock_collect_file_info, mock_list_dirs
):
    # Test Case 1: No backup directories exist, backup is needed
    mock_list_dirs.return_value = []  # No directories found
    result = _backup_needed("source_path", "destination_path")
    assert result is True  # Backup should be needed

    # Test Case 2: Directories exist but no changes in files
    mock_list_dirs.return_value = ["BU-2024-12-11"]
    mock_collect_file_info.side_effect = [
        (True, "source_output_file"),
        (True, "destination_output_file"),
    ]  # Mock source and destination outputs
    mock_compare_files.return_value = True  # Files are the same
    result = _backup_needed("source_path", "destination_path")
    assert result is False  # Backup not needed
    mock_send_email.assert_called_once_with(
        "Backup not run.",
        "There was no need to backup files as the content hasn't changed.",
    )

    # Test Case 3: Directories exist, files differ
    mock_compare_files.return_value = False  # Files differ
    result = _backup_needed("source_path", "destination_path")
    assert result is True  # Backup is needed

    # Test Case 4: Error collecting file info
    mock_collect_file_info.side_effect = [
        (False, None),  # Error for source
        (True, "destination_output_file"),
    ]
    result = _backup_needed("source_path", "destination_path")
    assert result is True  # Assume backup is needed if source collection fails

