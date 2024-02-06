## Summarize the changes

%{all_commits}

## Ensure that

- [ ] New code is covered by unit and integration tests.
- [ ] Related issues would be automatically closed with
      gitlab's closing pattern (`Closes #1, #2`).
- [ ] Public modules are documented (check the rendered version with
      `cargo doc --open`).
- [ ] (if PEST grammar is changed) EBNF grammar reflects these changes
      (check the result with railroad diagram [generator].

## Next steps

- Update sbroad submodule in [picodata/picodata].
- (if EBNF grammar is changed) create a follow-up issue in [picodata/docs].

[generator]: https://www.bottlecaps.de/rr/ui
[picodata/docs]: https://git.picodata.io/picodata/picodata/docs/-/issues/new
[picodata/picodata]: https://git.picodata.io/picodata/picodata/picodata
