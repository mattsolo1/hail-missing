# hail-missing

Goals:

+ Analyze `hl.Table` for fields that contain missing values.
+ Identify rows with missing values and provide their keys for further examination or filtration.
+ Calculate the percentage of missing values for each field.
+ Assess the missing status of parent fields independently from their children fields.
    + For example, if `my.field` is occasionally absent, yet `my.field.nested_val` is always present whenever `my.field` is, then `nested_val` should not be considered missing. Its presence can be inferred from its parent.
    + Note: This rationale underpins the decision not to use `hl.flatten`, as it would remove the hierarchical relationship between fields.
+ Achieve efficient processing on large datasets by limiting to a single invocation of `Table.aggregate`.

## Known limitations

The tool is not compatible with tables that include empty `ArrayStructExpressions`. These should be converted to missing prior to analysis (refer to the example below).

## Example

```python
from hail_missing.rich_table import ht

# A complex nested hail table with missing values
ht.describe()
```

```
----------------------------------------
Global fields:
    None
----------------------------------------
Row fields:
    'k1': str
    'k2': str
    'a': int32
    'b': str
    'c': bool
    'd': int64
    'e': float32
    'f': float64
    'g': call
    'h': locus<GRCh38>
    'i': array<int32>
    'j': array<struct {
        in_empty_array: str
    }>
    'complex_dictionary': dict<str, int32>
    'k': set<int32>
    'detailed_struct': struct {
        long_field1: int32,
        long_field2: str
    }
    'array_of_structs': array<struct {
        long_n: int32,
        long_o: str,
        inner_array_of_structs: array<struct {
            inner_n: int32,
            inner_o: str,
            inner_s: struct {
                another_field: str
            }
        }>
    }>
    'nested_complex_struct': struct {
        q: int32,
        detailed_struct: struct {
            long_field1: int32,
            long_field2: str
        },
        inner_struct: struct {
            long_s: int32,
            long_t: str
        }
    }
    'optional_field': int32
    'deeply_nested_struct': struct {
        outer_field: struct {
            inner_field1: int32,
            inner_field2: str
        }
    }
----------------------------------------
Key: ['k1', 'k2']
----------------------------------------`
```

### Creating a missingness report

```python
from pathlib import Path

import hail as hl
import pandas as pd
from hail_missing.missingness import MissingnessReport

# Remove empty values in `ArrayStructExpression`
ht = ht.annotate(j=hl.or_missing(hl.len(ht.j) > 0, ht.j))

r = MissingnessReport(ht, cache_path=Path('./result.csv'))

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
r.df

```

E.g. result

```
Out[29]:
                                                field  counts                    missing_keys  missing_percent
0                                                  k1       0                              []              0.0
1                                                  k2       0                              []              0.0
2                                                   a       0                              []              0.0
3                                                   b       0                              []              0.0
4                                                   c       0                              []              0.0
5                                                   d       0                              []              0.0
6                                                   e       0                              []              0.0
7                                                   f       0                              []              0.0
8                                                   g       0                              []              0.0
9                                                   h       0                              []              0.0
10                                                  i       0                              []              0.0
11                                 complex_dictionary       0                              []              0.0
12                                                  k       0                              []              0.0
13                                    detailed_struct       0                              []              0.0
14                        detailed_struct.long_field1       1  [{'k1': 'key3', 'k2': 'key4'}]             50.0
15                        detailed_struct.long_field2       0                              []              0.0
16                                   array_of_structs       0                              []              0.0
17                            array_of_structs.long_n       1                              []             50.0
18                            array_of_structs.long_o       0                              []              0.0
19    array_of_structs.inner_array_of_structs.inner_n       1  [{'k1': 'key1', 'k2': 'key2'}]             50.0
20    array_of_structs.inner_array_of_structs.inner_o       0                              []              0.0
21    array_of_structs.inner_array_of_structs.inner_s       0                              []              0.0
22  array_of_structs.inner_array_of_structs.inner_...       1  [{'k1': 'key3', 'k2': 'key4'}]             50.0
23            array_of_structs.inner_array_of_structs       0                              []              0.0
24                              nested_complex_struct       0                              []              0.0
25                            nested_complex_struct.q       0                              []              0.0
26              nested_complex_struct.detailed_struct       0                              []              0.0
27  nested_complex_struct.detailed_struct.long_field1       1  [{'k1': 'key3', 'k2': 'key4'}]             50.0
28  nested_complex_struct.detailed_struct.long_field2       0                              []              0.0
29                 nested_complex_struct.inner_struct       0                              []              0.0
30          nested_complex_struct.inner_struct.long_s       0                              []              0.0
31          nested_complex_struct.inner_struct.long_t       1  [{'k1': 'key3', 'k2': 'key4'}]             50.0
32                                     optional_field       1  [{'k1': 'key3', 'k2': 'key4'}]             50.0
33                               deeply_nested_struct       1  [{'k1': 'key3', 'k2': 'key4'}]             50.0
34                   deeply_nested_struct.outer_field       0                              []              0.0
35      deeply_nested_struct.outer_field.inner_field1       0                              []              0.0
36      deeply_nested_struct.outer_field.inner_field2       1  [{'k1': 'key1', 'k2': 'key2'}]             50.0
```
