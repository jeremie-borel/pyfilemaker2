# PyFileMaker - Integrating FileMaker and Python
* (c) 2016-2021 Jeremie Borel
* (c) 2014-2016 Marcin Kawa, kawa.macin@gmail.com
* (c) 2006-2008 Klokan Petr Pridal, klokan@klokan.cz
* (c) 2002-2006 Pieter Claerhout, pieter@yellowduck.be

Old project urls:

* https://github.com/aeguana/PyFileMaker
* http://code.google.com/p/pyfilemaker/
* http://www.yellowduck.be/filemaker/

-------------------------------------------------------------------------------

### TABLE OF CONTENTS

1. What is pyfilemaker2 ?
2. Requirements
3. How to install pyfilemaker2
4. Documentation
5. Changes

-------------------------------------------------------------------------------
### 1. WHAT IS pyfilemaker2?

pyfilemaker2 is based, but largely rewritten, from PyFileMaker 
(https://github.com/aeguana/PyFileMaker) whose description is still valid:

```
PyFileMaker is a set of Python modules that makes it easy to access and modify
data stored in a FileMaker Pro/Server database. You can use it to query a FileMaker
database, but you can also use it to add data to a FileMaker database, you
can even use it to delete records and execute FileMaker scripts.
```

pyfilemaker2 implements the following additional features:
- FM server responses can be streamed and parsed 'on the fly' during the streaming. 
- FM server responses can be automatically paginated as large dataset (>20k records) 
  tend to trigger FMS and/or network timeouts.
- meant to be thread safe, though I can't claim to be an expert on the subject...
- More consistent and finer control on the way data types are cast when reading or 
  writting FM records (in particular dates, datetimes and float objects)
- python 3 compatible
- improved test battery

----

### 2. REQUIREMENTS

At the time of the development (2021), pyfilemaker2 is using:

```
requests=2.25.1
lxml=4.6.2
```

The test suite also requires

```
mock
pytz
```

The code has been developped on Max OSX 10.15 and is used in production on 
debian jessie. Other plateforms have not been tested.

You will also need a FileMaker server with the XML enabled of course.

----

### 3. HOW TO INSTALL PYFILEMAKER2

You can install from pypi using pip

```
$ pip install pyfilemaker2
```

---

### 4. DOC

Their is no external doc but the function in the FmServer class have extensive
docstring. Starts in particular with the server.py file. Moreover an FmServer
object is likely the unique thing one will need to import from this package.

### 5. CHANGES

0.2.2:

- BackCast now cast numbers correctly

0.2.1:

- Bugfix on import in test_server.py


0.2.0:

- No longer support python2.7 as the `future` library was removed

0.1.14:

- Minor change in FmServer.get_file to allow downloading of 
  badly formatted filenames.

0.1.13:

- changed the BackCast function so that query parameters may be
  bytestring 

0.1.12: 

- improving this readme

- bug fix in the FmServer.fetched_records_number function

0.1.11: enforcing stream=True by default as FMS now requires it.

0.1.10: bug fix with the stream argument.

0.1.9: 

- adding timezone support as FM datetime object are naive datetime

- changed the way the options are passed to FmServer object
         so that a subclass with default
         parameters can be used.
         
0.1.6: bug fixes in `do_find_query`

Version 0.1
 - First release of the code
