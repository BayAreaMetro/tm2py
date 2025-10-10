#!/usr/bin/env python3
"""
Unit Tests for TM2PY Network Summary Script

Tests the key functions in network_summary.py including:
- Time period detection from scenario titles
- Performance metric calculations (VMT, VHT, delay)
- Data validation functions

Usage:
    pytest test_network_summary.py -v
    
Or run standalone:
    python test_network_summary.py
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch
import sys
from pathlib import Path
import tempfile
import shutil

# Add scripts directory to path to import network_summary
script_dir = Path(__file__).parent.resolve()
scripts_dir = script_dir.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

from network_summary import NetworkSummarizer


class TestTimeperiodMapping:
    """Test time period detection from scenario titles."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.test_dir = Path.cwd() / "test_output_pytest"
        self.test_dir.mkdir(exist_ok=True)
        self.generator = NetworkSummarizer(str(self.test_dir))
    
    def teardown_method(self):
        """Clean up test fixtures."""
        if self.test_dir.exists():
            try:
                shutil.rmtree(self.test_dir)
            except:
                pass  # Ignore cleanup errors
    
    def test_morning_time_periods(self):
        """Test AM time period detection."""
        # Test various AM scenario title formats
        mock_scenario = Mock()
        
        # Standard AM titles
        mock_scenario.title = "AM Peak Hour"
        assert self.generator._map_scenario_to_time_period(mock_scenario) == 'am'
        
        mock_scenario.title = "Morning Rush"
        assert self.generator._map_scenario_to_time_period(mock_scenario) == 'am'
        
        mock_scenario.title = "7-9 AM Period"
        assert self.generator._map_scenario_to_time_period(mock_scenario) == 'am'
        
        # Case insensitive
        mock_scenario.title = "am_peak_scenario"
        assert self.generator._map_scenario_to_time_period(mock_scenario) == 'am'
        
        mock_scenario.title = "MORNING_PERIOD"
        assert self.generator._map_scenario_to_time_period(mock_scenario) == 'am'
    
    def test_afternoon_time_periods(self):
        """Test PM time period detection."""
        mock_scenario = Mock()
        
        # Standard PM titles
        mock_scenario.title = "PM Peak Hour"
        assert self.generator._map_scenario_to_time_period(mock_scenario) == 'pm'
        
        mock_scenario.title = "Afternoon Rush"
        assert self.generator._map_scenario_to_time_period(mock_scenario) == 'pm'
        
        mock_scenario.title = "4-7 PM Period"
        assert self.generator._map_scenario_to_time_period(mock_scenario) == 'pm'
    
    def test_midday_time_periods(self):
        """Test MD time period detection."""
        mock_scenario = Mock()
        
        mock_scenario.title = "MD Off-Peak"
        assert self.generator._map_scenario_to_time_period(mock_scenario) == 'md'
        
        mock_scenario.title = "Midday Period"
        assert self.generator._map_scenario_to_time_period(mock_scenario) == 'md'
    
    def test_early_time_periods(self):
        """Test EA time period detection.""" 
        mock_scenario = Mock()
        
        mock_scenario.title = "EA Early Morning"
        assert self.generator._map_scenario_to_time_period(mock_scenario) == 'ea'
        
        mock_scenario.title = "Early Period"
        assert self.generator._map_scenario_to_time_period(mock_scenario) == 'ea'
    
    def test_evening_time_periods(self):
        """Test EV time period detection."""
        mock_scenario = Mock()
        
        mock_scenario.title = "EV Evening"
        assert self.generator._map_scenario_to_time_period(mock_scenario) == 'ev'
        
        mock_scenario.title = "Night Period"
        assert self.generator._map_scenario_to_time_period(mock_scenario) == 'ev'
    
    def test_unknown_time_periods(self):
        """Test handling of unrecognized scenario titles."""
        mock_scenario = Mock()
        
        # Unknown/unrecognized titles
        mock_scenario.title = "Special Scenario"
        assert self.generator._map_scenario_to_time_period(mock_scenario) == 'unknown'
        
        mock_scenario.title = "Test Run"
        assert self.generator._map_scenario_to_time_period(mock_scenario) == 'unknown'
        
        # Empty or None titles
        mock_scenario.title = ""
        assert self.generator._map_scenario_to_time_period(mock_scenario) == 'unknown'
        
        mock_scenario.title = None
        assert self.generator._map_scenario_to_time_period(mock_scenario) == 'unknown'


class TestPerformanceMetrics:
    """Test performance metric calculations."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.test_dir = Path.cwd() / "test_output_pytest2"
        self.test_dir.mkdir(exist_ok=True)
        self.generator = NetworkSummarizer(str(self.test_dir))
        
        # Create sample data for testing
        self.sample_data = pd.DataFrame({
            'link_id': [1, 2, 3, 1, 2, 3],
            'time_period': ['am', 'am', 'am', 'ea', 'ea', 'ea'],
            'volume': [1000, 1500, 500, 100, 200, 50],
            'length': [1.0, 2.0, 0.5, 1.0, 2.0, 0.5],
            'auto_time': [2.0, 4.0, 1.0, 1.5, 3.0, 0.8],  # minutes
            'num_lanes': [2, 3, 1, 2, 3, 1],
            'facility_type': [1, 2, 7, 1, 2, 7]
        })
    
    def teardown_method(self):
        """Clean up test fixtures."""
        if self.test_dir.exists():
            try:
                shutil.rmtree(self.test_dir)
            except:
                pass  # Ignore cleanup errors
    
    def test_vmt_calculation(self):
        """Test Vehicle Miles Traveled calculation."""
        result_df = self.generator._calculate_performance_metrics(self.sample_data.copy())
        
        # VMT = volume * length
        expected_vmt = [1000*1.0, 1500*2.0, 500*0.5, 100*1.0, 200*2.0, 50*0.5]
        
        assert 'vmt' in result_df.columns
        np.testing.assert_array_equal(result_df['vmt'].values, expected_vmt)
    
    def test_vht_calculation(self):
        """Test Vehicle Hours Traveled calculation."""
        result_df = self.generator._calculate_performance_metrics(self.sample_data.copy())
        
        # VHT = volume * auto_time / 60 (convert minutes to hours)
        expected_vht = [1000*2.0/60, 1500*4.0/60, 500*1.0/60, 100*1.5/60, 200*3.0/60, 50*0.8/60]
        
        assert 'vht' in result_df.columns
        np.testing.assert_array_almost_equal(result_df['vht'].values, expected_vht, decimal=6)
    
    def test_lane_miles_calculation(self):
        """Test lane miles calculation."""
        result_df = self.generator._calculate_performance_metrics(self.sample_data.copy())
        
        # Lane miles = length * num_lanes
        expected_lane_miles = [1.0*2, 2.0*3, 0.5*1, 1.0*2, 2.0*3, 0.5*1]
        
        assert 'lane_miles' in result_df.columns
        np.testing.assert_array_equal(result_df['lane_miles'].values, expected_lane_miles)
    
    def test_speed_calculation(self):
        """Test speed calculation in mph."""
        result_df = self.generator._calculate_performance_metrics(self.sample_data.copy())
        
        assert 'speed_mph' in result_df.columns
        # All speeds should be positive for our test data
        assert all(result_df['speed_mph'] >= 0)
    
    def test_delay_calculation(self):
        """Test delay calculation using EA as freeflow."""
        result_df = self.generator._calculate_performance_metrics(self.sample_data.copy())
        
        # Delay should be calculated relative to EA (freeflow) times
        assert 'total_delay' in result_df.columns
        assert 'delay_per_vehicle' in result_df.columns
        
        # All delays should be non-negative
        assert all(result_df['total_delay'] >= 0)
        assert all(result_df['delay_per_vehicle'] >= 0)
    
    def test_required_columns_present(self):
        """Test that all required columns are created."""
        result_df = self.generator._calculate_performance_metrics(self.sample_data.copy())
        
        required_columns = ['vmt', 'vht', 'lane_miles', 'speed_mph', 'total_delay', 'delay_per_vehicle']
        for col in required_columns:
            assert col in result_df.columns, f"Missing required column: {col}"


class TestDataValidation:
    """Test data validation functions."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.test_dir = Path.cwd() / "test_output_pytest3"
        self.test_dir.mkdir(exist_ok=True)
        self.generator = NetworkSummarizer(str(self.test_dir))
    
    def teardown_method(self):
        """Clean up test fixtures."""
        if self.test_dir.exists():
            try:
                shutil.rmtree(self.test_dir)
            except:
                pass  # Ignore cleanup errors
    
    def test_valid_dataframe(self):
        """Test validation of good dataframe."""
        good_df = pd.DataFrame({
            'link_id': [1, 2, 3],
            'volume': [1000, 1500, 500],
            'length': [1.0, 2.0, 0.5],
            'auto_time': [2.0, 4.0, 1.0],
            'time_period': ['am', 'pm', 'md']
        })
        
        assert self.generator._validate_extracted_data(good_df) == True
    
    def test_empty_dataframe(self):
        """Test validation of empty dataframe."""
        empty_df = pd.DataFrame()
        
        assert self.generator._validate_extracted_data(empty_df) == False


class TestOutputValidation:
    """Test output validation functions."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.test_dir = Path.cwd() / "test_output_validation"
        self.test_dir.mkdir(exist_ok=True)
        self.generator = NetworkSummarizer(str(self.test_dir))
        
        # Create mock output files for testing
        self._create_mock_output_files()
    
    def teardown_method(self):
        """Clean up test fixtures."""
        if self.test_dir.exists():
            try:
                shutil.rmtree(self.test_dir)
            except:
                pass  # Ignore cleanup errors
    
    def _create_mock_output_files(self):
        """Create mock output files for testing."""
        # Create facility type summary with realistic data
        facility_data = pd.DataFrame({
            'facility_type': ['freeway', 'arterial', 'collector', 'local'],
            'vmt': [50000000, 30000000, 15000000, 5000000],  # Realistic VMT values
            'vht': [1000000, 1500000, 750000, 500000],       # Realistic VHT values
            'links': [5000, 15000, 10000, 20000]
        })
        facility_data.to_csv(self.generator.output_dir / 'facility_type_summary.csv', index=False)
        
        # Create overall summary with key metrics
        overall_data = pd.DataFrame({
            'Metric': ['Total VMT', 'Total VHT', 'Average Speed', 'Total Lane Miles'],
            'Value': [100000000, 3750000, 26.7, 75000]  # Realistic values
        })
        overall_data.to_csv(self.generator.output_dir / 'overall_summary.csv', index=False)
        
        # Create lane mile inventory
        lane_data = pd.DataFrame({
            'facility_type': ['freeway', 'arterial', 'collector', 'local'],
            'lane_miles': [15000, 25000, 20000, 15000]
        })
        lane_data.to_csv(self.generator.output_dir / 'lane_mile_inventory.csv', index=False)
    
    def test_validate_output_files_success(self):
        """Test successful output file validation."""
        result = self.generator._validate_output_files()
        
        assert result['status'] == 'pass'
        assert len(result['errors']) == 0
        assert result['details']['files_found'] == result['details']['files_checked']
    
    def test_validate_output_files_missing(self):
        """Test output file validation with missing files."""
        # Remove one file
        (self.generator.output_dir / 'facility_type_summary.csv').unlink()
        
        result = self.generator._validate_output_files()
        
        assert result['status'] == 'fail'
        assert len(result['errors']) > 0
        assert any('Missing output file' in error for error in result['errors'])
    
    def test_validate_speed_ranges_success(self):
        """Test successful speed range validation."""
        result = self.generator._validate_speed_ranges()
        
        assert result['status'] in ['pass', 'pass_with_warnings']  # May have warnings
        assert 'details' in result
        assert result['details']['facility_types_analyzed'] > 0
    
    def test_validate_speed_ranges_unrealistic(self):
        """Test speed range validation with unrealistic speeds."""
        # Create facility data with unrealistic speeds
        bad_facility_data = pd.DataFrame({
            'facility_type': ['freeway', 'arterial'],
            'vmt': [1000000, 1000000],
            'vht': [100000, 10000],  # This will create 10 mph and 100 mph speeds
            'links': [1000, 1000]
        })
        bad_facility_data.to_csv(self.generator.output_dir / 'facility_type_summary.csv', index=False)
        
        result = self.generator._validate_speed_ranges()
        
        assert len(result['warnings']) > 0 or result['status'] == 'fail'
    
    def test_validate_vmt_vht_ratios_success(self):
        """Test successful VMT/VHT ratio validation."""
        result = self.generator._validate_vmt_vht_ratios()
        
        assert result['status'] in ['pass', 'pass_with_warnings']
        assert 'details' in result
        assert result['details']['ratios_analyzed'] > 0
    
    def test_validate_vmt_vht_impossible_condition(self):
        """Test VMT/VHT validation with impossible conditions."""
        # Create data with VMT > 0 but VHT = 0 (impossible)
        bad_data = pd.DataFrame({
            'facility_type': ['freeway'],
            'vmt': [1000000],
            'vht': [0],  # Impossible: travel distance with no travel time
            'links': [1000]
        })
        bad_data.to_csv(self.generator.output_dir / 'facility_type_summary.csv', index=False)
        
        result = self.generator._validate_vmt_vht_ratios()
        
        assert len(result['errors']) > 0
        assert any('impossible' in error.lower() for error in result['errors'])
    
    def test_validate_data_completeness_success(self):
        """Test successful data completeness validation."""
        result = self.generator._validate_data_completeness()
        
        assert result['status'] == 'pass'
        assert len(result['errors']) == 0
        assert all(details['completeness_pct'] == 100.0 for details in result['details'].values())
    
    def test_validate_data_completeness_missing_values(self):
        """Test data completeness validation with missing values."""
        # Create data with missing values
        incomplete_data = pd.DataFrame({
            'facility_type': ['freeway', 'arterial', None],  # Missing value
            'vmt': [1000000, None, 500000],                  # Missing value
            'vht': [50000, 100000, 25000],
            'links': [1000, 2000, 1500]
        })
        incomplete_data.to_csv(self.generator.output_dir / 'facility_type_summary.csv', index=False)
        
        result = self.generator._validate_data_completeness()
        
        # Should have warnings about missing values
        assert len(result['warnings']) > 0 or result['status'] == 'fail'
    
    def test_validate_value_ranges_success(self):
        """Test successful value range validation."""
        result = self.generator._validate_value_ranges()
        
        assert result['status'] in ['pass', 'pass_with_warnings']
        assert 'details' in result
    
    def test_validate_value_ranges_unrealistic(self):
        """Test value range validation with unrealistic values."""
        # Create overall summary with unrealistic values
        bad_overall_data = pd.DataFrame({
            'Metric': ['Total VMT', 'Total VHT', 'Average Speed'],
            'Value': [10000, 1000000, 150]  # Very low VMT, high VHT, very high speed
        })
        bad_overall_data.to_csv(self.generator.output_dir / 'overall_summary.csv', index=False)
        
        result = self.generator._validate_value_ranges()
        
        assert len(result['warnings']) > 0 or result['status'] == 'fail'
    
    def test_validate_outputs_comprehensive(self):
        """Test comprehensive output validation."""
        result = self.generator.validate_outputs()
        
        assert 'status' in result
        assert 'checks' in result
        assert 'summary' in result
        assert result['status'] in ['pass', 'pass_with_warnings', 'fail']
        assert result['summary']['total_checks'] > 0


if __name__ == "__main__":
    # Run tests if called directly (without pytest)
    import unittest
    
    print("Running network_summary.py unit tests...")
    
    # Create a simple test runner for direct execution
    def run_test_class(test_class):
        suite = unittest.TestSuite()
        for method_name in dir(test_class):
            if method_name.startswith('test_'):
                suite.addTest(test_class(method_name))
        runner = unittest.TextTestRunner(verbosity=2)
        return runner.run(suite)
    
    # This allows the file to be run directly for quick testing
    # For full pytest functionality, use: pytest test_network_summary.py -v
    print("For full test functionality, run: pytest test_network_summary.py -v")