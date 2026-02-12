# StarRocks Project Guide for AI Agents

This document provides essential information about the StarRocks project for AI coding agents.

## Project Overview

StarRocks is a high-performance, distributed SQL query engine for sub-second, ad-hoc analytics on data lakehouses. It's a Linux Foundation project licensed under Apache License 2.0.

### Key Features
- Native vectorized SQL engine with sub-second query performance
- Standard ANSI SQL support with MySQL protocol compatibility
- Smart query optimization using CBO (Cost-Based Optimizer)
- Real-time updates with upsert/delete operations
- Intelligent materialized views with automatic updates
- Direct querying of data lakes (Hive, Iceberg, Delta Lake, Hudi)
- Resource management for multi-tenant isolation
- Shared-data architecture (from version 3.0)

## Technology Stack

### Primary Languages
- **Java 17** - Frontend (FE) and Java extensions
- **C++** - Backend (BE) with vectorized execution
- **Python 3.8+** - Testing framework and utilities

### Build Tools
- **Frontend**: Apache Maven 3.6+
- **Backend**: CMake 3.11+, GCC 12.1+, Ninja (optional)
- **Java Extensions**: Apache Maven

### Key Dependencies
- **FE**: Apache Thrift, Protobuf, Netty, Jackson, Hadoop, Spark, Hive, Iceberg, Hudi, Paimon
- **BE**: Boost, LLVM, jemalloc, gperftools, OpenSSL, BRPC, Arrow

## Architecture

StarRocks uses a streamlined two-module architecture:

### Frontend (FE)
- **Purpose**: SQL parsing, query planning, optimization, and metadata management
- **Language**: Java 17
- **Key Components**:
  - SQL parser and analyzer
  - Cost-Based Optimizer (CBO)
  - Query planner
  - Metadata management (catalogs, databases, tables)
  - Cluster management
- **Modules**:
  - `fe-core`: Main FE logic
  - `fe-common`: Common utilities
  - `fe-spi`: Service provider interface
  - `fe-parser`: SQL parsing
  - `fe-utils`: Utility functions
  - `spark-dpp`: Spark data preprocessing
  - `hive-udf`: Hive UDF support
  - `plugin-common`: Plugin framework

### Backend (BE)
- **Purpose**: Query execution, storage management, and data processing
- **Language**: C++
- **Key Components**:
  - Vectorized execution engine
  - Storage engine (columnar format)
  - Query execution operators
  - Data ingestion
  - Compaction and maintenance
- **Main Directories**:
  - `src/exec`: Query execution operators
  - `src/storage`: Storage engine
  - `src/exprs`: Expression evaluation
  - `src/formats`: Data format readers/writers
  - `src/runtime`: Runtime components
  - `src/service`: Service interfaces
  - `src/connector`: Connectors for external data sources
  - `src/fs`: File system abstractions

### Java Extensions
Java extensions provide connectivity to external systems:
- `jdbc-bridge`: JDBC connector
- `hudi-reader`: Apache Hudi integration
- `paimon-reader`: Apache Paimon integration
- `hive-reader`: Apache Hive integration
- `iceberg-metadata-reader`: Apache Iceberg integration
- `kudu-reader`: Apache Kudu integration
- `odps-reader`: MaxCompute (ODPS) integration
- `udf-extensions`: UDF support
- `jni-connector`: JNI bridge
- `hadoop-ext`: Hadoop extensions

## Project Structure

```
starrocks/
├── be/                     # Backend (C++) source code
│   ├── src/               # Source files
│   ├── test/              # Unit tests
│   └── CMakeLists.txt     # CMake configuration
├── fe/                     # Frontend (Java) source code
│   ├── fe-core/           # Main FE module
│   ├── fe-common/         # Common utilities
│   ├── spark-dpp/         # Spark data preprocessing
│   ├── hive-udf/          # Hive UDFs
│   ├── pom.xml            # Maven parent POM
│   └── checkstyle.xml     # Java code style
├── java-extensions/        # Java extensions for external connectors
│   └── pom.xml            # Maven parent POM
├── gensrc/                 # Code generation (Thrift, Protobuf)
│   ├── proto/             # Protobuf definitions
│   └── thrift/            # Thrift definitions
├── test/                   # SQL integration testing framework
│   ├── sql/               # SQL test cases
│   ├── run.py             # Test runner
│   └── requirements.txt   # Python dependencies
├── thirdparty/             # Third-party dependencies
├── conf/                   # Configuration files
│   ├── fe.conf            # FE configuration
│   ├── be.conf            # BE configuration
│   └── cn.conf            # Compute node configuration
├── bin/                    # Startup scripts
├── docker/                 # Docker configurations
├── docs/                   # Documentation
└── build.sh               # Main build script
```

## Build Commands

### Prerequisites
- GCC 12.1+ or Clang
- Java 17 (JDK)
- Maven 3.6+
- CMake 3.11+
- Python 3.8+
- Thrift and Protobuf compilers

### Full Build
```bash
./build.sh                    # Build FE, BE, Spark DPP, and Hive UDF
```

### Component-Specific Builds
```bash
./build.sh --fe              # Build Frontend only
./build.sh --be              # Build Backend only
./build.sh --spark-dpp       # Build Spark DPP only
./build.sh --hive-udf        # Build Hive UDF only
./build.sh --format-lib      # Build format library (shared-data mode)
```

### Build Options
```bash
./build.sh --clean           # Clean before build
./build.sh --be --clean      # Clean and build BE
BUILD_TYPE=Debug ./build.sh --be    # Debug build (Release, Debug, ASAN)
./build.sh --be --enable-shared-data # Build with shared-data support
./build.sh --be --with-gcov  # Build with code coverage
./build.sh --be --with-bench # Build with benchmarks
./build.sh --be --with-clang-tidy   # Build with clang-tidy
DISABLE_JAVA_CHECK_STYLE=ON ./build.sh  # Skip Java checkstyle
```

### Docker Build
```bash
docker-compose -f docker-compose.dev.yml run --rm starrocks-dev
# Inside container:
./build.sh
```

## Test Commands

### Frontend Unit Tests
```bash
./run-fe-ut.sh                          # Run all FE unit tests
./run-fe-ut.sh --test TestClassName     # Run specific test class
./run-fe-ut.sh --filter TestClass#method # Exclude specific test
./run-fe-ut.sh --coverage               # Run with coverage
./run-fe-ut.sh -j16                     # Run with 16 parallel threads
```

### Backend Unit Tests
```bash
./run-be-ut.sh                          # Run all BE unit tests
./run-be-ut.sh --test TestName          # Run specific test
./run-be-ut.sh --gtest_filter "Filter"  # Use gtest filter
./run-be-ut.sh --clean                  # Clean and run
./run-be-ut.sh --with-gcov              # Run with coverage
./run-be-ut.sh --enable-shared-data     # Test with shared-data
```

### Java Extensions Tests
```bash
cd java-extensions && mvn test
```

### SQL Integration Tests
```bash
cd test
pip install -r requirements.txt
# Configure conf/sr.conf with database connection
python3 run.py                          # Run concurrent cases
python3 run.py -a sequential -c 1 -v    # Run sequential cases
python3 run.py -r                       # Record mode (generate results)
python3 run.py -v                       # Validate mode (compare results)
```

## Code Style Guidelines

### Java
- Follow Google Java Style Guide (modified)
- Line length: 130 characters maximum
- Indentation: 4 spaces
- Checkstyle configuration: `fe/checkstyle.xml`
- Maven checkstyle plugin runs during build

### C++
- Based on Google C++ Style Guide
- Line length: 120 characters maximum
- Indentation: 4 spaces
- Pointer alignment: Left (`int* ptr`)
- Configuration: `.clang-format`

### Formatting Commands
```bash
# Format C++ code
clang-format -i be/src/path/to/file.cpp

# Java formatting is enforced by checkstyle during Maven build
```

## Development Conventions

### Commit Messages
- Write in English
- Start with a verb clause in uppercase
- Use concise, explicit language
- Common prefixes: `[BugFix]`, `[Enhancement]`, `[Feature]`, `[UT]`, `[Doc]`, `[Tool]`, `[Refactor]`

Example:
```
[BugFix] Fix memory leak in column reader
[Enhancement] Optimize hash join performance
[Feature] Add support for JSON type
```

### PR Requirements
- Title must follow format: `[BugFix]`, `[Enhancement]`, `[Feature]`, `[UT]`, `[Doc]`, `[Tool]`, `[Refactor]`, or `Revert`
- Relate to an issue in the PR body
- Submit one commit per PR (recommended)
- Use the PR template: `.github/PULL_REQUEST_TEMPLATE.md`
- All CI checks must pass

### Code Organization
- FE code: `fe/fe-core/src/main/java/com/starrocks/`
- BE code: `be/src/`
- Protocol definitions: `gensrc/proto/` and `gensrc/thrift/`
- Tests:
  - FE: `fe/fe-core/src/test/java/`
  - BE: `be/test/`
  - SQL: `test/sql/`

## Testing Strategy

### Unit Testing
- **FE**: JUnit 5 with Maven Surefire plugin
- **BE**: Google Test (gtest)
- Mocking: JMockit for Java

### Integration Testing
- SQL-tester framework for end-to-end SQL testing
- Test data: SSB, TPC-DS, TPC-H datasets
- Record/validate mode for result verification

### Coverage
- FE: JaCoCo for coverage reports
- BE: gcov for coverage
- Coverage workflows run on CI

## CI/CD

### GitHub Actions Workflows
- `ci-pipeline.yml`: Main CI pipeline for PRs
- `ci-pipeline-branch.yml`: Branch-specific CI
- `pr-checker.yml`: PR validation
- `inspection-fe-coverage.yml`: FE coverage inspection
- `inspection-be-coverage.yml`: BE coverage inspection
- `sonarcloud-be-build.yml`: SonarCloud analysis

### CI Checks
- Title format validation
- Checkstyle (Java)
- Unit tests (FE and BE)
- SQL test cases
- SonarCloud analysis

## Configuration Files

### Key Configuration Files
- `fe/pom.xml`: Maven parent POM for FE
- `fe/checkstyle.xml`: Java code style rules
- `fe/starrocks_intellij_style.xml`: IntelliJ IDEA formatter
- `be/CMakeLists.txt`: CMake build configuration
- `.clang-format`: C++ formatting rules
- `env.sh`: Build environment setup
- `conf/fe.conf`: Frontend runtime configuration
- `conf/be.conf`: Backend runtime configuration

## Security Considerations

- UDF security policy: `conf/udf_security.policy`
- Check for security vulnerabilities in dependencies (Trivy scanner)
- `.trivyignore`: Vulnerability exclusions
- Apache Ranger integration for access control (in `conf/ranger/`)

## Important Notes

1. **Third-party Libraries**: Must be built and installed in `thirdparty/` before building StarRocks
2. **Generated Code**: Protobuf and Thrift code is generated during build via `gensrc/`
3. **Parallel Build**: Use `-j` flag to control parallelism
4. **Build Cache**: ccache is automatically used if available
5. **Docker Support**: Development environment available via docker-compose

## Resources

- **Documentation**: https://docs.starrocks.io/
- **Community**: https://starrocks.io/
- **Slack**: https://try.starrocks.com/join-starrocks-on-slack
- **Issues**: https://github.com/StarRocks/starrocks/issues
- **Contributing Guide**: `CONTRIBUTING.md`

## License

Apache License 2.0 - See `LICENSE.txt` for details.
