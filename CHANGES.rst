0.24:
- @hitz needs to add entries here for the changes he already merged
- If the schema specifies an explicit `mapping`,
  use it when building the elasticsearch mapping.
  This provides an escape valve for edge cases
  (such as not indexing the layout structure of a page).

0.23:
- replace copy.deepcopy() for faster indexing

0.22:
- New version of image magic, fix sauce labs

0.21:
- (pypi errors, identical to 0.22)

0.20:
- Patch mpindexer for better error messages
