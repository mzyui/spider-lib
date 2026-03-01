# Contributing to spider-lib

Thank you for your interest in contributing to spider-lib! This document provides guidelines and instructions for contributing to the project.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Making Changes](#making-changes)
- [Testing](#testing)
- [Code Style](#code-style)
- [Pull Request Process](#pull-request-process)
- [Issue Reporting](#issue-reporting)

## Code of Conduct

Please be respectful and constructive in your interactions. We welcome contributions from everyone regardless of experience level.

## Getting Started

1. **Fork** the repository on GitHub
2. **Clone** your fork locally:
   ```bash
   git clone https://github.com/mzyui/spider-lib.git
   cd spider-lib
   ```
3. **Add upstream** remote:
   ```bash
   git remote add upstream https://github.com/mzyui/spider-lib.git
   ```

## Development Setup

### Prerequisites

- Rust 1.70 or later (install via [rustup](https://rustup.rs/))
- Cargo package manager

### Building the Project

```bash
# Build with default features
cargo build

# Build with all features
cargo build --all-features

# Build a specific crate
cargo build -p spider-core
```

### Running Examples

```bash
cargo run --example books
```

## Making Changes

1. **Create a branch** from main:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes** following the code style guidelines below

3. **Test your changes** thoroughly (see [Testing](#testing))

4. **Commit your changes** with clear, descriptive messages (see [Commit Messages](#commit-messages))

5. **Push** to your fork:
   ```bash
   git push origin feature/your-feature-name
   ```

## Testing

### Running Tests

```bash
# Run all tests
cargo test

# Run tests with all features
cargo test --all-features

# Run tests for a specific crate
cargo test -p spider-core

# Run tests with output
cargo test -- --nocapture
```

### Running Clippy

```bash
# Run clippy without features
cargo clippy

# Run clippy with all features
cargo clippy --all-features

# Run clippy with warnings as errors (CI check)
cargo clippy --all-features -- -D warnings
```

### Running fmt

```bash
# Check formatting
cargo fmt --check

# Format code
cargo fmt
```

### Pre-commit Checklist

Before submitting a PR, ensure:

- [ ] `cargo test --all-features` passes
- [ ] `cargo clippy --all-features` has no warnings
- [ ] `cargo fmt --check` passes
- [ ] `cargo build --all-features` succeeds
- [ ] New code has appropriate tests
- [ ] Documentation is updated if needed

## Code Style

### General Guidelines

- Follow the [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)
- Use `cargo fmt` (rustfmt) for consistent formatting
- Keep functions focused and small
- Prefer descriptive names over comments
- Use Rust's type system to prevent errors

### Documentation

- Document all public items with `///` comments
- Include examples in documentation when helpful
- Use `#[derive(Debug)]` for user-facing types when appropriate
- Update README.md if adding significant features

### Error Handling

- Use `Result<T, SpiderError>` for fallible operations
- Provide meaningful error messages
- Use `?` operator for error propagation
- Avoid `unwrap()` in library code; use `expect()` with context if necessary

### Async Code

- Use `async/await` for asynchronous operations
- Mark async functions with `async` keyword
- Use `tokio` runtime features consistently
- Document async behavior in function documentation

## Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/) format:

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

**Example:**
```
feat(pipeline): add JSONL pipeline output

Add support for JSON Lines format output in the pipeline module.
This allows streaming JSON output for large datasets.

Closes #123
```

## Pull Request Process

1. **Ensure your branch is up to date**:
   ```bash
   git fetch upstream
   git rebase upstream/main
   ```

2. **Squash commits** if you have multiple small commits that should be one

3. **Create a Pull Request** on GitHub with:
   - Clear title following commit message format
   - Description of changes
   - Reference to related issues (e.g., "Closes #123")
   - List of any breaking changes

4. **Address review feedback** promptly

5. **Wait for CI** to pass before merging

### PR Requirements

- All CI checks must pass
- At least one maintainer approval required
- No merge conflicts with main
- Changelog entry for user-facing changes (if applicable)

## Issue Reporting

### Before Reporting

- Search existing issues to avoid duplicates
- Check if the issue exists in the latest version

### Bug Reports

Include:
- **Description**: Clear description of the bug
- **Steps to Reproduce**: Minimal code example if possible
- **Expected Behavior**: What should happen
- **Actual Behavior**: What actually happens
- **Environment**: Rust version, OS, spider-lib version
- **Logs**: Relevant error messages or stack traces

### Feature Requests

Include:
- **Description**: What feature you want
- **Use Case**: Why you need it
- **Examples**: How it would be used
- **Alternatives**: Any workarounds you've tried

## Architecture Overview

The project is organized as a workspace with multiple crates:

| Crate | Purpose |
|-------|---------|
| `spider-core` | Core engine, scheduler, crawler |
| `spider-downloader` | HTTP downloaders |
| `spider-macro` | Procedural macros |
| `spider-middleware` | Request/response middleware |
| `spider-pipeline` | Data processing pipelines |
| `spider-util` | Shared utilities and types |

When making changes, consider which crate(s) are affected and ensure compatibility.

## Feature Flags

When adding new optional functionality:

1. Add a feature flag in `Cargo.toml`
2. Use `#[cfg(feature = "your-feature")]` guards
3. Document the feature in README.md
4. Ensure tests cover both with and without the feature

## Questions?

If you have questions before contributing:
- Open a GitHub Discussion
- Check existing documentation
- Look at example code in `examples/`

Thank you for contributing to spider-lib! 🕷️
