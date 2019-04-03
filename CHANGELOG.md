<a name="0.3.2"></a>
# [0.3.2](https://github.com/qri-io/startf/compare/v0.3.1...v0.3.2) (2019-04-03)

This release includes a breaking change described by [RFC0023](https://github.com/qri-io/rfcs/blob/master/text/0023-starlark_load_dataset.md). Datasets are now loaded with `load_dataset` and `qri.load_dataset` is removed.

### Features

* **ds:** add get_meta method that gets all dataset metadata ([d2dedcd](https://github.com/qri-io/startf/commit/d2dedcd))
* **ModuleLoader:** allow supplying custom module loader ([f8c85b9](https://github.com/qri-io/startf/commit/f8c85b9))


### BREAKING CHANGES

* **ModuleLoader:** qri.load_dataset is no longer available, use load_dataset instead



<a name="0.3.0"></a>
# [0.3.0](https://github.com/qri-io/startf/compare/v0.2.1...v0.3.0) (2019-03-07)

a few small tweaks, and a version bump to match [starlib](https://github.com/qri-io/starlib)

### Bug Fixes

* **ds.set_body:** don't assign schemas when using set_body ([df4742b](https://github.com/qri-io/startf/commit/df4742b))
* **set_meta:** use GoString to set meta keys ([af7a305](https://github.com/qri-io/startf/commit/af7a305))


### Features

* **ds:** structure component getter & setter methods ([0dc0d41](https://github.com/qri-io/startf/commit/0dc0d41))
* **ds.set_body:** accept data_format argument in conjunction with raw=True ([3f12e40](https://github.com/qri-io/startf/commit/3f12e40))


### BREAKING CHANGES

* **ds:** ds.set_schema is removed, use ds.set_structure instead.



<a name="0.2.1"></a>
# [0.2.1](https://github.com/qri-io/startf/compare/v0.2.0...v0.2.1) (2019-02-05)


### Features

* **ds.get_body:** return None on empty body, support optional default body ([85fa909](https://github.com/qri-io/startf/commit/85fa909))



<a name="0.2.0"></a>
# [0.2.0](https://github.com/qri-io/startf/compare/v0.1.0...v0.2.0) (2019-01-22)


### Bug Fixes

* **empty body:** remove empty body array assumption ([de5d458](https://github.com/qri-io/startf/commit/de5d458))
* update to fix starlib util.AsString change ([2f965c6](https://github.com/qri-io/startf/commit/2f965c6))


### Features

* **context:** add context package ([cd70f69](https://github.com/qri-io/startf/commit/cd70f69))
* **context:** add get_config, get_secret funcs to context ([4a41689](https://github.com/qri-io/startf/commit/4a41689))
* **MutateCheck:** add option for dataset mutation checks, ExecFile -> ExecScript ([c7e65a3](https://github.com/qri-io/startf/commit/c7e65a3))
* **OutWriter:** accept writer param on ExecTransform to record script stdout ([19e7c43](https://github.com/qri-io/startf/commit/19e7c43))



<a name="0.0.2"></a>
# [0.0.2](https://github.com/qri-io/skytf/compare/v0.0.1...v0.0.2) (2018-06-18)


### Features

* **html:** added quick jquery-like html package ([921bbc2](https://github.com/qri-io/skytf/commit/921bbc2))
* **NtwkToggle:** explicit network use toggling in http package ([df9acb5](https://github.com/qri-io/skytf/commit/df9acb5))
* **set_shcmea:** added set_schema method to qri module ([a38b402](https://github.com/qri-io/skytf/commit/a38b402))



<a name="0.0.1"></a>
## [0.0.1](https://github.com/qri-io/skytf/compare/9c030ce...v0.0.1) (2018-06-06)


### Bug Fixes

* **http.auth,qri.get_config:** fix silly bugs from recent changes ([6f8ac60](https://github.com/qri-io/skytf/commit/6f8ac60))


### Features

* **EntryReader:** add support for list, dict, tuple reading ([2343bde](https://github.com/qri-io/skytf/commit/2343bde))
* **error, set_met:** added error & set_meta methods ([fa5ed07](https://github.com/qri-io/skytf/commit/fa5ed07))
* **ExecFile:** Execute skylark file, get qri dataset. dope. ([9c030ce](https://github.com/qri-io/skytf/commit/9c030ce))
* **http:** overhaul http module ([4cedf6f](https://github.com/qri-io/skytf/commit/4cedf6f))
* **lib:** qri.get_body function for updates ([9be0e4a](https://github.com/qri-io/skytf/commit/9be0e4a))
* **Protector:** stepped transform functions, protected execution ([f03e409](https://github.com/qri-io/skytf/commit/f03e409))
* **Version:** write SyntaxVersion on ExecTransform ([bbd4975](https://github.com/qri-io/skytf/commit/bbd4975))



