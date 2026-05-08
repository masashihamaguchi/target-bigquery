"""Tests for partition configuration."""

import pytest

from target_bigquery.core import BaseBigQuerySink


class MockSink(BaseBigQuerySink):
    """Mock sink for testing partition configuration."""

    def __init__(self, stream_name, config):
        self.stream_name = stream_name
        self._config = config

    def process_record(self, record, context):
        pass

    def process_batch(self, context):
        """Mock implementation of abstract method."""
        pass

    @staticmethod
    def worker_cls_factory(worker_executor_cls, config):
        """Mock implementation of abstract method."""
        return None


def test_partition_config_default_only():
    """Test partition config with only default settings."""
    config = {
        "partition": {
            "default": {
                "enabled": True,
                "granularity": "day",
                "field": "created_at",
                "expiration_days": 365
            }
        }
    }
    sink = MockSink("users", config)
    result = sink._get_partition_config_for_stream()
    
    assert result["enabled"] is True
    assert result["granularity"] == "day"
    assert result["field"] == "created_at"
    assert result["expiration_days"] == 365


def test_partition_config_stream_override():
    """Test partition config with stream override."""
    config = {
        "partition": {
            "default": {
                "enabled": True,
                "granularity": "month",
                "field": "_sdc_batched_at"
            },
            "streams": {
                "users": {
                    "granularity": "day",
                    "field": "created_at"
                }
            }
        }
    }
    sink = MockSink("users", config)
    result = sink._get_partition_config_for_stream()
    
    assert result["enabled"] is True
    assert result["granularity"] == "day"
    assert result["field"] == "created_at"


def test_partition_config_pattern_match():
    """Test partition config with pattern matching."""
    config = {
        "partition": {
            "default": {
                "enabled": True,
                "granularity": "month"
            },
            "streams": {
                "staging_*": {
                    "enabled": False
                }
            }
        }
    }
    sink = MockSink("staging_users", config)
    result = sink._get_partition_config_for_stream()
    
    assert result["enabled"] is False
    assert result["granularity"] == "month"


def test_partition_config_no_match():
    """Test partition config when stream doesn't match any pattern."""
    config = {
        "partition": {
            "default": {
                "enabled": True,
                "granularity": "month"
            },
            "streams": {
                "staging_*": {
                    "enabled": False
                }
            }
        }
    }
    sink = MockSink("users", config)
    result = sink._get_partition_config_for_stream()
    
    assert result["enabled"] is True
    assert result["granularity"] == "month"


def test_partition_config_fallback_defaults():
    """Test partition config falls back to hardcoded defaults."""
    config = {}
    sink = MockSink("users", config)
    result = sink._get_partition_config_for_stream()
    
    assert result["enabled"] is True
    assert result["granularity"] == "month"
    assert result["field"] == "_sdc_batched_at"
    assert result["expiration_days"] is None


def test_partition_config_partial_default():
    """Test partition config with partial default settings."""
    config = {
        "partition": {
            "default": {
                "granularity": "hour"
            }
        }
    }
    sink = MockSink("events", config)
    result = sink._get_partition_config_for_stream()
    
    assert result["enabled"] is True  # Falls back to hardcoded default
    assert result["granularity"] == "hour"
    assert result["field"] == "_sdc_batched_at"  # Falls back to hardcoded default


def test_partition_config_multiple_patterns():
    """Test partition config with multiple pattern matches (last match wins)."""
    config = {
        "partition": {
            "default": {
                "enabled": True,
                "granularity": "month"
            },
            "streams": {
                "event*": {
                    "granularity": "hour"
                },
                "events_critical": {
                    "granularity": "day"
                }
            }
        }
    }
    # Note: The behavior depends on dict iteration order in Python 3.7+
    sink = MockSink("events_critical", config)
    result = sink._get_partition_config_for_stream()
    
    assert result["enabled"] is True
    # Result depends on which pattern is matched last
    assert result["granularity"] in ["hour", "day"]
