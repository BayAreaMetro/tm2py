
# CT-RAMP UEC Processing Framework Analysis
**Generated:** 2025-09-26 08:03:25
**Purpose:** Analyze Java UEC framework to design Python equivalent

## Executive Summary

- **Core UEC classes identified:** 27
- **File reading classes:** 78
- **Expression processors:** 18
- **DMU classes:** 39
- **Data table classes:** 30

## Core UEC Processing Classes

### Constants
- **Package:** `com.pb.common.calculator2`
- **Size:** 63 lines
- **Component:** None

### DataEntry
- **Package:** `com.pb.common.calculator2`
- **Size:** 51 lines
- **Component:** None

### Expression
- **Package:** `com.pb.common.calculator2`
- **Size:** 979 lines
- **Component:** None

### ExpressionFlags
- **Package:** `com.pb.common.calculator2`
- **Size:** 33 lines
- **Component:** None

### ExpressionIndex
- **Package:** `com.pb.common.calculator2`
- **Size:** 66 lines
- **Component:** None

### IndexValues
- **Package:** `com.pb.common.calculator2`
- **Size:** 35 lines
- **Component:** None

### LinkCalculator
- **Package:** `com.pb.common.calculator`
- **Size:** 228 lines
- **Component:** None

### LinkFunction
- **Package:** `com.pb.common.calculator`
- **Size:** 191 lines
- **Component:** None

### MatrixCalculator
- **Package:** `com.pb.common.calculator`
- **Size:** 241 lines
- **Component:** None

### MatrixDataManager
- **Package:** `com.pb.common.calculator2`
- **Size:** 618 lines
- **Component:** mandatory_tour

## UEC File Reading Infrastructure

### ControlFileReader
- **Package:** `com.pb.mtctm2.abm.ctramp`
- **Excel Imports:** 3
- **Key Imports:** jxl.Sheet, jxl.Workbook, jxl.WorkbookSettings

### LinkCalculator
- **Package:** `com.pb.common.calculator`
- **Excel Imports:** 1
- **Key Imports:** com.pb.common.datafile.TableDataSet

### TableDataSetManager
- **Package:** `com.pb.common.calculator2`
- **Excel Imports:** 2
- **Key Imports:** com.pb.common.datafile.CSVFileReader, com.pb.common.datafile.TableDataSet

### UtilityExpressionCalculator
- **Package:** `com.pb.common.newmodel`
- **Excel Imports:** 3
- **Key Imports:** com.pb.common.calculator.TableDataSetManager, com.pb.common.datafile.OLD_CSVFileReader, com.pb.common.datafile.TableDataSet

### BaseDataFile
- **Package:** `com.pb.common.datafile`
- **Excel Imports:** 0

## Expression Processing Infrastructure

### Expression
- **Package:** `com.pb.common.calculator2`
- **Type:** expression_parser
- **Size:** 979 lines

### ExpressionFlags
- **Package:** `com.pb.common.calculator2`
- **Type:** expression_parser
- **Size:** 33 lines

### ExpressionIndex
- **Package:** `com.pb.common.calculator2`
- **Type:** expression_parser
- **Size:** 66 lines

### LinkCalculator
- **Package:** `com.pb.common.calculator`
- **Type:** calculator
- **Size:** 228 lines

### MatrixCalculator
- **Package:** `com.pb.common.calculator`
- **Type:** calculator
- **Size:** 241 lines

## Common Import Patterns

Most frequently used UEC-related imports:

- `com.pb.common.datafile.TableDataSet` (used 49 times)
- `com.pb.common.calculator.VariableTable` (used 48 times)
- `com.pb.common.calculator.IndexValues` (used 30 times)
- `com.pb.common.matrix.Matrix` (used 25 times)
- `com.pb.common.calculator.MatrixDataServerIf` (used 25 times)
- `com.pb.common.datafile.OLD_CSVFileReader` (used 23 times)
- `com.pb.common.matrix.MatrixType` (used 20 times)
- `com.pb.common.datafile.CSVFileReader` (used 15 times)
- `com.pb.common.calculator.MatrixDataManager` (used 15 times)
- `com.pb.common.matrix.MatrixReader` (used 11 times)

## Python Framework Design

### uec_reader
**Purpose:** Read and parse Excel UEC files

**Java Equivalent:** TableDataSet, DataFile classes

**Key Functions:**
- `read_excel_uec()`
- `parse_uec_sheets()`
- `extract_expressions()`

### expression_parser
**Purpose:** Parse and compile UEC expressions

**Java Equivalent:** Expression, Calculator classes

**Key Functions:**
- `parse_expression()`
- `compile_expression()`
- `evaluate_expression()`

### dmu_manager
**Purpose:** Manage Decision Making Unit data

**Java Equivalent:** DMU classes

**Key Functions:**
- `create_dmu()`
- `populate_dmu()`
- `get_dmu_value()`

### utility_calculator
**Purpose:** Calculate utilities and choice probabilities

**Java Equivalent:** UtilityCalculator, ChoiceModel

**Key Functions:**
- `calculate_utilities()`
- `apply_logsum()`
- `make_choice()`

### matrix_handler
**Purpose:** Handle matrix data (skims, etc.)

**Java Equivalent:** MatrixDataServer, Matrix classes

**Key Functions:**
- `load_matrix()`
- `get_matrix_value()`
- `interpolate()`

### Core Data Structures

- **UECSpec:** Specification of a UEC model
- **DMU:** Decision Making Unit data container
- **ExpressionTree:** Parsed expression for evaluation
- **UtilityResults:** Results of utility calculations
- **ChoiceResults:** Results of discrete choice modeling

### Python Dependencies

- **pandas:** Data manipulation and Excel reading
- **numpy:** Numerical calculations and arrays
- **openpyxl:** Excel file reading (if .xlsx)
- **xlrd:** Excel file reading (if .xls)
- **numba:** JIT compilation for performance
- **scipy:** Statistical functions and optimization

## Performance Considerations

### Key Bottlenecks
- Expression evaluation in tight loops
- Matrix operations on large datasets
- File I/O for large Excel files
- Memory usage with large choice sets

### Optimization Strategies
- Compile expressions once, evaluate many times
- Use numpy/scipy for vectorized operations
- Cache matrix values and intermediate results
- Use numba for performance-critical loops
- Lazy loading of data files

### Memory Management
- Stream large Excel files instead of loading all
- Use memory-mapped matrices when possible
- Clear intermediate results when not needed
- Use generators for large datasets

## Implementation Roadmap

### Phase 1 Foundation
**Description:** Core UEC reading and basic expression parsing

**Estimated Effort:** 2-3 weeks

**Deliverables:**
- Excel UEC file reader
- Basic expression parser
- Simple DMU class
- Unit tests for core functionality

### Phase 2 Processing
**Description:** Expression evaluation and utility calculation

**Estimated Effort:** 3-4 weeks

**Deliverables:**
- Expression evaluator with variables
- Utility calculator
- Choice model framework
- Matrix data integration

### Phase 3 Integration
**Description:** Full model integration and optimization

**Estimated Effort:** 2-3 weeks

**Deliverables:**
- Complete UEC processor
- Performance optimizations
- Error handling and validation
- Integration with existing models

### Phase 4 Validation
**Description:** Validation against Java implementation

**Estimated Effort:** 1-2 weeks

**Deliverables:**
- Comprehensive test suite
- Performance benchmarks
- Validation reports
- Documentation


## Next Steps for Step 6

Based on this framework analysis:

1. **Start with Phase 1:** Excel UEC reader and basic expression parser
2. **Validate approach:** Test with simple UEC files from high-confidence mappings
3. **Iterative development:** Build and test each component incrementally
4. **Performance focus:** Implement optimizations early in the process

### Recommended Starting Points:
1. **AutoOwnership.xls** -> Simple UEC structure, clear Java processor
2. **Accessibilities.xls** -> Well-defined accessibility calculations  
3. **StopFrequency.xls** -> Frequency choice model pattern

---
*This framework analysis provides the technical foundation for Python UEC implementation.*
