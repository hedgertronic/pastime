# Pastime for Python

Tools for acquiring and analyzing baseball data from sources such as [Baseball Savant](https://baseballsavant.mlb.com), [Fangraphs](https://www.fangraphs.com), and [Baseball Reference](https://www.baseball-reference.com).

## Installation

`pastime` can be installed via pip:

`pip install pastime`

## Getting Started

Due to the large file size, the Chadwick Bureau lookup table is not installed by default. To download it, follow these steps:

### 1. Determine installation location of `pastime`

`pip show pastime`

This should print something like this to the screen:

```
Name: pastime
...
Location: <INSTALLATION_LOCATION>
```

### 2. Create data directory

`mkdir "<INSTALLATION_LOCATION>/pastime/data"`

### 3. Download table into data directory

`python -m pastime.lookup --table --refresh -o "<INSTALLATION_LOCATION>/pastime/data/lookup_table.csv"`

If you ever believe that the table is out of date and want to update it, re-run the command from step 3.
