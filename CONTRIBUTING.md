# Contributing to Finviz Weekly Scraper

Thank you for your interest in contributing! This document provides guidelines and instructions for contributing to this project.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [How to Contribute](#how-to-contribute)
- [Pull Request Process](#pull-request-process)
- [Coding Standards](#coding-standards)
- [Testing Guidelines](#testing-guidelines)
- [Commit Message Guidelines](#commit-message-guidelines)

## Code of Conduct

- Be respectful and inclusive
- Focus on constructive feedback
- Help maintain a welcoming environment for all contributors

## Getting Started

### Prerequisites

- Python 3.11 or higher
- Git
- (Optional) Docker for containerized development

### Development Setup

1. **Fork and clone the repository:**
   ```bash
   git fork https://github.com/alienorsutinn/finviz-scraper
   cd finviz-scraper
   ```

2. **Create a virtual environment:**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -e .[dev]
   ```

4. **Set up environment variables:**
   ```bash
   cp .env.example .env
   # Edit .env with your API keys
   ```

5. **Install pre-commit hooks (optional but recommended):**
   ```bash
   pip install pre-commit
   pre-commit install
   ```

6. **Run tests to verify setup:**
   ```bash
   pytest tests/ -v
   ```

## How to Contribute

### Reporting Bugs

Before creating bug reports, please check existing issues. When creating a bug report, include:

- **Clear title and description**
- **Steps to reproduce**
- **Expected vs actual behavior**
- **Python version and OS**
- **Relevant logs or error messages**

### Suggesting Enhancements

Enhancement suggestions are tracked as GitHub issues. When creating an enhancement suggestion, include:

- **Clear title and description**
- **Use case and motivation**
- **Proposed implementation approach**
- **Any alternative solutions considered**

### Contributing Code

1. **Pick an issue** or create a new one to discuss your changes
2. **Create a branch** from `main`:
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. **Make your changes** following our [coding standards](#coding-standards)
4. **Write tests** for new functionality
5. **Run tests** to ensure everything works:
   ```bash
   pytest tests/ -v
   ```
6. **Commit your changes** with clear messages
7. **Push** to your fork and submit a pull request

## Pull Request Process

1. **Update documentation** if you're changing functionality
2. **Add tests** for new features or bug fixes
3. **Ensure all tests pass** (`pytest tests/`)
4. **Update CHANGELOG.md** with notable changes
5. **Link related issues** in the PR description
6. **Request review** from maintainers

### PR Checklist

- [ ] Tests added/updated and passing
- [ ] Documentation updated
- [ ] Code follows style guidelines
- [ ] Commit messages follow conventions
- [ ] No merge conflicts
- [ ] CHANGELOG.md updated (if applicable)

## Coding Standards

### Python Style

We follow **PEP 8** with some modifications:

- **Line length:** 120 characters (not 79)
- **Imports:** Use `isort` for organizing imports
- **Formatting:** Use `black` for code formatting
- **Type hints:** Use type annotations for all functions

### Code Organization

```python
# Good example
from typing import List
from pathlib import Path

def scrape_ticker(ticker: str, session: Session, config: HttpConfig) -> dict:
    """
    Scrape fundamental data for a single ticker.
    
    Args:
        ticker: Stock ticker symbol (e.g., "AAPL")
        session: Requests session with configured headers
        config: HTTP configuration for rate limiting
        
    Returns:
        Dictionary containing fundamental metrics
        
    Raises:
        ValueError: If ticker is invalid
        HTTPError: If scraping fails after retries
    """
    # Implementation
    pass
```

### Docstring Format

Use **Google-style docstrings** for all public functions:

```python
def function_name(arg1: Type1, arg2: Type2) -> ReturnType:
    """Brief one-line description.
    
    More detailed explanation if needed.
    
    Args:
        arg1: Description of arg1
        arg2: Description of arg2
        
    Returns:
        Description of return value
        
    Raises:
        ExceptionType: When this exception is raised
    """
```

## Testing Guidelines

### Test Structure

```
tests/
├── test_parse.py           # Unit tests for parsing utilities
├── test_screener.py        # Tests for screener scraping
├── test_pipeline.py        # Tests for pipeline logic
├── test_screen.py          # Tests for scoring logic
├── test_debate.py          # Tests for AI layer
└── fixtures/               # Test fixtures and mock data
    ├── html_samples/
    └── parquet_samples/
```

### Writing Tests

```python
import pytest
from finviz_weekly.parse import parse_percent

def test_parse_percent_valid():
    """Test parsing valid percentage strings."""
    assert parse_percent("12.5%") == 0.125
    assert parse_percent("100%") == 1.0
    assert parse_percent("-5%") == -0.05

def test_parse_percent_invalid():
    """Test parsing invalid percentage strings."""
    assert parse_percent("N/A") is None
    assert parse_percent("") is None
    
@pytest.mark.parametrize("input,expected", [
    ("10%", 0.1),
    ("0%", 0.0),
    ("invalid", None),
])
def test_parse_percent_parametrized(input, expected):
    """Parametrized test for multiple cases."""
    assert parse_percent(input) == expected
```

### Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_parse.py -v

# Run with coverage report
pytest tests/ --cov=src/finviz_weekly --cov-report=html

# Run tests matching pattern
pytest tests/ -k "test_parse" -v
```

## Commit Message Guidelines

We follow **Conventional Commits** format:

```
<type>(<scope>): <subject>

<body>

<footer>
```

### Types

- **feat:** New feature
- **fix:** Bug fix
- **docs:** Documentation changes
- **style:** Code style changes (formatting, no logic change)
- **refactor:** Code refactoring (no functionality change)
- **test:** Adding or updating tests
- **chore:** Maintenance tasks (dependencies, build, etc.)
- **perf:** Performance improvements

### Examples

```bash
# Feature
feat(screener): add support for international stocks

Implement scraping for LSE and TSE exchanges.
Adds new get_international_tickers() function.

Closes #123

# Bug fix
fix(pipeline): handle empty industry responses

Prevents crash when Finviz returns no tickers for an industry.
Falls back to next industry instead of failing.

Fixes #456

# Documentation
docs(readme): add Docker setup instructions

Add section explaining how to run scraper in Docker container.
Includes docker-compose example.

# Refactoring
refactor(scoring): simplify percentile calculation

Replace custom percentile logic with pandas.qcut().
Improves readability without changing behavior.
```

## Development Workflow

### Feature Development

```bash
# 1. Create feature branch
git checkout -b feature/my-feature

# 2. Make changes
# ... edit files ...

# 3. Run tests
pytest tests/ -v

# 4. Commit changes
git add .
git commit -m "feat(scope): add my feature"

# 5. Push to fork
git push origin feature/my-feature

# 6. Create pull request on GitHub
```

### Bug Fixes

```bash
# 1. Create bugfix branch
git checkout -b fix/issue-123

# 2. Fix the bug
# ... edit files ...

# 3. Add test for the bug
# ... add regression test ...

# 4. Verify fix
pytest tests/ -v

# 5. Commit
git commit -m "fix(module): fix issue #123"

# 6. Push and create PR
git push origin fix/issue-123
```

## Areas Where We Need Help

### High Priority

- [ ] Add more integration tests
- [ ] Improve documentation with examples
- [ ] Add support for international markets
- [ ] Build data quality monitoring dashboard
- [ ] Implement backtesting framework

### Medium Priority

- [ ] Add more scoring factors
- [ ] Improve AI research synthesis
- [ ] Add Streamlit dashboard features
- [ ] Create Docker deployment guide
- [ ] Add performance benchmarks

### Good First Issues

- [ ] Add more unit tests
- [ ] Fix typos in documentation
- [ ] Improve error messages
- [ ] Add type hints to uncovered functions
- [ ] Update dependencies

## Questions?

- Create an issue for questions
- Check existing issues and discussions
- Read the README.md for basic usage

## License

By contributing, you agree that your contributions will be licensed under the same license as the project.
