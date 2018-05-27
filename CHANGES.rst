1.0.9:
01. SNO-38 Return lists from get_related_uuids (#108, #105)

1.0.8: Released with 1.0.9
01. SNO-36-update-buildout (#104)
02. SNO-34-nginx-dev-proxy-headers (#103)

1.0.7: The only update was reverted.  Empty Release.

1.0.6:
01. SNO-33 specify index for get_by_unique_key from collection (#94)
02. SNO-28 limit ES storage to indices created for snovault resources (#93)

1.0.5:
01. SNO-30 Split Indexer State from indexer file and update
02. SNO-10 Remove audit indexing via 2-pass
03. SNO-9 Add api end points to new indexer meta objects
04. SNO-25 Make uniqueItems to check the serialized values (#85)
05. SNO-26 Add schemas map view to profiles (#86)
06. SNO-29 Limit audits on form update (#87)
07. SNO-19 Update DOI preferred resolver url (#80)

1.0.4:
-SNO-15 Add index to storage propertysheet
-SNO-14 Update delete script

1.0.3:
-SNO-8 Add JSONSchemas type to profiles page (#73)


1.0.2: * Issues discovered while release of ENCD v65 part 2

-SNO-12: Set max clause parameter in es indexer search #75
-SNO-11: Add timeout to ES indexer search query #74

1.0.1: * Issues discovered while release of ENCD v65

-SNO-6: Fix index query too many clauses failure
-SNO-5: Update psycopg to match encoded version 2.7.3

1.0.0:

- 31 delete
- ES5 Fix: Update index settings shard number

0.33:

- ES5 Update: ENCD-2488 ES5 Update aka RM3910
- Fix travis build: Pre Install setuptools with pip for travis like ENCD-3722

0.32:

- Update to ENCD-3669 to not include notSubmittable
  reverse links in the edit view of an object.

0.31:

- ENCD-3684 Specify https index to fix buildout, update
  changelog.

- Use notSubmittable instead of calculatedProperty
  to indicate properties that may not be submitted.
  Related to ENCD-3669.

0.30:

- Document dependency on java 8.

- Disable 2nd indexing pass.

0.29:

- Fix recording indexing errors.

- Add some documentation about indexing.

0.28:

- Add support for adding and updating child objects
  specified as abstract types in the schema.

- Split indexing into 2 phases.

0.27:

- Move embed cache to connection and increase size.

- Fix reporting upgrade errors when error path includes an integer.

0.26:

- Indexer: Limit workers to 1 task and scale chunk size based on number of items being indexed.

0.25:

- Indexer: Limit workers to 4 tasks to avoid out-of-memory errors.

0.24:

- If the schema specifies an explicit `mapping`, use it when building the elasticsearch mapping.  This provides an escape valve for edge cases (such as not indexing the layout structure of a page).

- upgrade to sauceconnect v4.4.4 

- add port_range to wsgi_tests (mrmin)

0.23:
- replace copy.deepcopy() for faster indexing

0.22:
- New version of image magic, fix sauce labs

0.21:
- (pypi errors, identical to 0.22)

0.20:
- Patch mpindexer for better error messages
