"""
Unit tests for configuration manager
"""

import pytest
import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, mock_open

from utils.config_manager import UnifiedConfigManager
from utils.exceptions import ConfigurationError


@pytest.mark.unit
class TestConfigManager:
    """Test cases for ConfigManager class"""

    @pytest.fixture
    def sample_config(self):
        """Sample configuration data"""
        return {
            "database": {
                "url": "sqlite:///test.db",
                "echo": False
            },
            "api": {
                "host": "localhost",
                "port": 8000
            },
            "data_sources": {
                "baostock": {
                    "enabled": True,
                    "rate_limit": {
                        "requests_per_second": 1
                    }
                }
            }
        }

    @pytest.fixture
    def config_file(self, sample_config, temp_dir):
        """Create temporary config file"""
        config_path = temp_dir / "config.json"
        with open(config_path, 'w') as f:
            json.dump(sample_config, f)
        return config_path

    @pytest.fixture
    def config_manager(self, config_file):
        """Create ConfigManager instance"""
        return UnifiedConfigManager(str(config_file))

    def test_config_manager_initialization(self, config_manager):
        """Test ConfigManager initialization"""
        assert config_manager is not None
        assert hasattr(config_manager, 'get')
        assert hasattr(config_manager, 'set')
        assert hasattr(config_manager, 'get_all')

    def test_get_config_value(self, config_manager):
        """Test getting configuration values"""
        # Test getting top-level value
        assert config_manager.get("database.url") == "sqlite:///test.db"

        # Test getting nested value
        assert config_manager.get("api.host") == "localhost"
        assert config_manager.get("api.port") == 8000

        # Test getting non-existent value with default
        assert config_manager.get("nonexistent", "default") == "default"

    def test_get_config_value_without_default(self, config_manager):
        """Test getting non-existent value without default"""
        with pytest.raises(ConfigurationError):
            config_manager.get("nonexistent.key")

    def test_set_config_value(self, config_manager):
        """Test setting configuration values"""
        # Set new value
        config_manager.set("test.new_key", "test_value")
        assert config_manager.get("test.new_key") == "test_value"

        # Set existing value
        config_manager.set("api.host", "new_host")
        assert config_manager.get("api.host") == "new_host"

    def test_get_all_config(self, config_manager, sample_config):
        """Test getting all configuration"""
        all_config = config_manager.get_all()
        assert isinstance(all_config, dict)
        assert all_config == sample_config

    def test_get_section_config(self, config_manager):
        """Test getting configuration section"""
        api_config = config_manager.get_section("api")
        assert isinstance(api_config, dict)
        assert api_config["host"] == "localhost"
        assert api_config["port"] == 8000

    def test_has_config_key(self, config_manager):
        """Test checking if configuration key exists"""
        assert config_manager.has("database.url") is True
        assert config_manager.has("api.host") is True
        assert config_manager.has("nonexistent") is False

    def test_validate_config(self, config_manager):
        """Test configuration validation"""
        # Test valid configuration
        assert config_manager.validate() is True

    def test_invalid_config_file(self):
        """Test handling invalid config file"""
        with pytest.raises(ConfigurationError):
            UnifiedConfigManager("nonexistent_file.json")

    def test_malformed_config_file(self, temp_dir):
        """Test handling malformed config file"""
        config_path = temp_dir / "invalid.json"
        with open(config_path, 'w') as f:
            f.write("{ invalid json }")

        with pytest.raises(ConfigurationError):
            UnifiedConfigManager(str(config_path))

    def test_config_file_not_found(self, config_manager):
        """Test handling when config file is not found"""
        # This should use default configuration
        # Implementation depends on specific requirements

    def test_environment_variable_override(self, config_manager):
        """Test environment variable override"""
        # Test with mock environment variable
        with patch.dict('os.environ', {'QUOTE_API_HOST': 'env_host'}):
            # This depends on environment variable support implementation
            pass

    def test_config_file_reload(self, config_manager, sample_config):
        """Test reloading configuration file"""
        # Modify original config
        modified_config = sample_config.copy()
        modified_config["api"]["host"] = "reloaded_host"

        # Mock file reading with modified config
        with patch('builtins.open', mock_open(read_data=json.dumps(modified_config))):
            config_manager.reload()

        # Verify config was reloaded
        assert config_manager.get("api.host") == "reloaded_host"

    def test_config_file_save(self, config_manager, temp_dir):
        """Test saving configuration file"""
        # Modify config
        config_manager.set("test.save_key", "save_value")

        # Save to new file
        save_path = temp_dir / "saved_config.json"
        config_manager.save(str(save_path))

        # Verify file was saved
        with open(save_path, 'r') as f:
            saved_config = json.load(f)

        assert saved_config["test"]["save_key"] == "save_value"

    def test_nested_key_access(self, config_manager):
        """Test accessing deeply nested keys"""
        # Test multi-level nested access
        assert config_manager.get("data_sources.baostock.enabled") is True
        assert config_manager.get("data_sources.baostock.rate_limit.requests_per_second") == 1

        # Set deeply nested value
        config_manager.set("new.deeply.nested.key", "nested_value")
        assert config_manager.get("new.deeply.nested.key") == "nested_value"

    def test_config_type_conversion(self, config_manager):
        """Test configuration value type conversion"""
        # Test integer conversion
        config_manager.set("test.int_value", "123")
        assert config_manager.get("test.int_value", type=int) == 123

        # Test boolean conversion
        config_manager.set("test.bool_value", "true")
        assert config_manager.get("test.bool_value", type=bool) is True

        # Test list conversion
        config_manager.set("test.list_value", "a,b,c")
        assert config_manager.get("test.list_value", type=list) == ["a", "b", "c"]

    def test_config_validation_rules(self, config_manager):
        """Test configuration validation rules"""
        # Test required field validation
        required_fields = ["database.url", "api.port"]
        assert config_manager.validate_required(required_fields) is True

        # Test with missing required field
        with pytest.raises(ConfigurationError):
            config_manager.validate_required(["missing.required.field"])

    def test_config_default_values(self, config_manager):
        """Test configuration default values"""
        # Test getting default for missing keys
        assert config_manager.get("missing.key", default="default_value") == "default_value"

        # Test default value types
        assert config_manager.get("missing.int", default=42, type=int) == 42
        assert config_manager.get("missing.bool", default=True, type=bool) is True

    def test_config_schema_validation(self, config_manager):
        """Test configuration schema validation"""
        schema = {
            "type": "object",
            "properties": {
                "database": {
                    "type": "object",
                    "properties": {
                        "url": {"type": "string"}
                    },
                    "required": ["url"]
                }
            },
            "required": ["database"]
        }

        # Test valid schema
        assert config_manager.validate_schema(schema) is True

        # Test invalid schema (would need to modify config to make it invalid)

    def test_config_merge(self, config_manager):
        """Test merging configuration"""
        merge_config = {
            "api": {
                "host": "merged_host",
                "new_field": "merged_value"
            },
            "new_section": {
                "key": "value"
            }
        }

        config_manager.merge(merge_config)

        # Verify merged values
        assert config_manager.get("api.host") == "merged_host"
        assert config_manager.get("api.new_field") == "merged_value"
        assert config_manager.get("new_section.key") == "value"

    def test_config_watch_changes(self, config_manager):
        """Test watching for configuration changes"""
        # This would test file watching functionality
        # Implementation depends on whether file watching is supported
        pass

    def test_config_encryption(self, config_manager):
        """Test configuration value encryption"""
        # Test if sensitive values can be encrypted/decrypted
        # Implementation depends on encryption support
        pass

    def test_config_inheritance(self, config_manager):
        """Test configuration inheritance"""
        # Test if configurations can inherit from parent configs
        # Implementation depends on inheritance support
        pass

    def test_config_profiles(self, config_manager):
        """Test configuration profiles"""
        # Test switching between different configuration profiles
        # Implementation depends on profile support
        pass

    def test_config_export_import(self, config_manager, temp_dir):
        """Test exporting and importing configuration"""
        # Export configuration
        export_path = temp_dir / "exported_config.json"
        config_manager.export(str(export_path))

        # Verify export
        assert export_path.exists()

        # Import configuration
        new_config = UnifiedConfigManager(str(config_manager.config_file))
        new_config.import_config(str(export_path))

        # Verify import
        assert new_config.get_all() == config_manager.get_all()

    def test_config_backup_restore(self, config_manager, temp_dir):
        """Test configuration backup and restore"""
        # Create backup
        backup_path = temp_dir / "config_backup.json"
        config_manager.backup(str(backup_path))

        # Modify configuration
        config_manager.set("test.backup", "modified")

        # Restore from backup
        config_manager.restore(str(backup_path))

        # Verify restore
        assert not config_manager.has("test.backup")

    def test_memory_config_manager(self):
        """Test in-memory configuration manager"""
        # Test configuration manager without file backing
        memory_config = UnifiedConfigManager()

        # Should work without file
        memory_config.set("memory.test", "memory_value")
        assert memory_config.get("memory.test") == "memory_value"

    def test_concurrent_access(self, config_manager):
        """Test concurrent configuration access"""
        import threading

        results = []

        def set_config(key, value):
            config_manager.set(key, value)
            results.append(config_manager.get(key))

        # Create multiple threads
        threads = []
        for i in range(5):
            thread = threading.Thread(
                target=set_config,
                args=(f"concurrent.test.{i}", f"value_{i}")
            )
            threads.append(thread)
            thread.start()

        # Wait for all threads
        for thread in threads:
            thread.join()

        # Verify all operations completed
        assert len(results) == 5
        for i, result in enumerate(results):
            assert result == f"value_{i}"