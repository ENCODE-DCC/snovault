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
