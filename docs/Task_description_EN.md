Test assignment for candidates for the role of Junior Developer.
It is a simple ETL with different file formats.

**The task solution code must comply with [PEP8](https://www.python.org/dev/peps/pep-0008/)**

The task consists of two blocks:

1. [Basic](# basic) - the main task (mandatory requirement)
2. [Advanced](#advanced) - addition to the main task (desirable)

In addition, there is a set of [points for thinking giving bonuses](#bonuses)

You can only use the Python standard library tools.
In all cases, the program must be run from the terminal.

To check the operation of the program, input data and results are provided in the form of 6 files:

- input data:
  - csv_data_1.csv
  - csv_data_2.csv
  - json_data.json
  - xml_data.xml
- results **Basic**:
  - basic_results.tsv
- results **Advanced**:
  - advanced_results.tsv

## Task

There are four files: two `.csv`, one `.json` and one .`xml`.

The first `.csv` has the following structure:

| D1  | D2  | ... | Dn  | M1  | M2  | ... | Mn  |
| :-: | :-: | :-: | :-: | :-: | :-: | :-: | :-: |
|  s  |  s  | ... |  s  |  i  |  i  | ... |  i  |
| ... | ... | ... | ... | ... | ... | ... | ... |

The second `.csv` has the following structure:

| D1  | D2  | ... | Dn  | M1  | M2  | ... | Mn  | ... | Mz  |
| :-: | :-: | :-: | :-: | :-: | :-: | :-: | :-: | :-: | :-: |
|  s  |  s  | ... |  s  |  i  |  i  | ... |  i  | ... |  i  |
| ... | ... | ... | ... | ... | ... | ... | ... | ... | ... |

**Attention! Column order may not match. Both files have headers.**

The `.json` file has the following structure:

```python
{
  "fields": [
    {
      "D1": "s",
      "D2": "s",
      ...
      "Dn": "s",
      "M1": i,
      ...
      "Mp": i,
    },
    ...
  ]
}
```

The `.xml` file contains the following structure:

```xml
<objects>
  <object name="D1">
    <value>s</value>
  </object>
  <object name="D2">
    <value>s</value>
  </object>
  ...
  <object name="Dn">
    <value>s</value>
  </object>
  <object name="M1">
    <value>i</value>
  </object>
  <object name="M2">
    <value>i</value>
  </object>
  ...
  <object name="Mn">
    <value>i</value>
  </object>
</objects>
```

Where _z_ > _n_, _p_ >= _n_, _s_ is a string and _i_ is an integer.

### Basic

The files must be transformed into one `.tsv` file with the following structure:

| D1  | D2  | ... | Dn  | M1  | M2  | ... | Mn  |
| :-: | :-: | :-: | :-: | :-: | :-: | :-: | :-: |
|  s  |  s  | ... |  s  |  i  |  i  | ... |  i  |
| ... | ... | ... | ... | ... | ... | ... | ... |

It must be sorted by the **D1** column and contain data from all four files.

### Advanced

The files must be transformed into one `.tsv` file with the following structure:

| D1  | D2  | ... | Dn  | MS1 | MS2 | ... | MSn |
| :-: | :-: | :-: | :-: | :-: | :-: | :-: | :-: |
|  s  |  s  | ... |  s  |  i  |  i  | ... |  i  |
| ... | ... | ... | ... | ... | ... | ... | ... |

Columns **MS1** ... **MSn** should contain the sums of values corresponding to **M1** ... **Mn** from 4 files grouped by unique values of combinations of lines from **D1** .. . **Dn**.

##### Example

**Content of .tsv file with data from 4 files:**

| D1  | D2  | M1  | M2  | M3  |
| :-: | :-: | :-: | :-: | :-: |
|  a  |  a  |  0  |  0  |  0  |
|  a  |  a  |  1  |  0  |  1  |
|  a  |  a  |  0  |  2  |  1  |
|  a  |  b  |  1  |  1  |  1  |
|  c  |  c  |  7  |  7  |  7  |

**Expected Result:**

| D1  | D2  | M1  | M2  | M3  |
| :-: | :-: | :-: | :-: | :-: |
|  a  |  a  |  1  |  2  |  2  |
|  a  |  b  |  1  |  1  |  1  |
|  c  |  c  |  7  |  7  |  7  |

### Bonuses

There are a few things to try to take into account:

- a correctly solved problem is not the main criterion for evaluation. It is VERY important not to forget about other characteristics of the code, such as maintainability, readability, extensibility.
- in the further use of the program, there may be a requirement for working with other types of files, for example `.yaml`.
- input files can be large
- the ability to process strings with incorrect values without terminating the program execution, informing the user about errors at the end of its execution
- think about organizing the testing of the program
