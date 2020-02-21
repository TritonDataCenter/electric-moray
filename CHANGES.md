# electric-moray Changelog

## 1.2.0

- [#13](https://github.com/joyent/electric-moray/issues/13) MANTA-4992 Update
  electric-moray fast server to use node-fast 3.0.0. Node-fast version 3.0.0
  moves the fast protocol version to version 2. The fast client communications
  with moray are handled by node-moray 3.7.0 and are not changed as part of this
  release.  This means that this version of electric-moray can communicate with
  moray servers using older versions of node-fast as well as moray servers that
  are updated to use node-fast 3.0.0.
