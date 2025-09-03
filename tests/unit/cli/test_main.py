"""
Tests for CLI main module
"""

import pytest
from click.testing import CliRunner
from pythia.cli.main import cli, info


class TestCLI:
    """Test CLI main functionality"""

    def setup_method(self):
        """Setup for each test"""
        self.runner = CliRunner()

    def test_cli_help(self):
        """Test CLI help output"""
        result = self.runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "Pythia - Python Worker Framework CLI" in result.output
        assert "create" in result.output
        assert "run" in result.output
        assert "monitor" in result.output

    def test_cli_version(self):
        """Test CLI version option"""
        result = self.runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        assert "0.1.0" in result.output

    def test_info_command(self):
        """Test info command"""
        result = self.runner.invoke(cli, ["info"])
        assert result.exit_code == 0
        assert "Pythia - Python Worker Framework" in result.output
        assert "Version: 0.1.0" in result.output
        assert "Available commands:" in result.output
        assert "create" in result.output
        assert "run" in result.output
        assert "monitor" in result.output
        assert "validate" in result.output

    def test_invalid_command(self):
        """Test invalid command"""
        result = self.runner.invoke(cli, ["invalid-command"])
        assert result.exit_code != 0
        assert "No such command" in result.output

    def test_cli_context_creation(self):
        """Test that CLI creates context properly"""
        # This tests the ctx.ensure_object(dict) functionality
        result = self.runner.invoke(cli, ["info"])
        assert result.exit_code == 0

    def test_command_registration(self):
        """Test that all commands are properly registered"""
        result = self.runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        
        # Check that all expected commands are listed
        expected_commands = ["create", "run", "monitor", "validate", "info"]
        for cmd in expected_commands:
            assert cmd in result.output