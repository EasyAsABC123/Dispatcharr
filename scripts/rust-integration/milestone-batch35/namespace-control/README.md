# Actual installed namespace package control

Prepared for Root/Ops insertion into the existing next Candidate35 Python-host package proof. No probe, interpreter, application, compiler or Cargo command was executed during preparation. Original Dispatcharr, Django and CPython authors retain their credit and licenses. The frozen two-path namespace fix remains unchanged.

Root can pass the exact bytes of `namespace_guest.py` as the next existing Python-host guest `-c` argument. Use the actual production `/usr/local/bin/dispatcharr-python` on the verified next Candidate35 immutable image, `-I -B`, UID/GID65532, network none/read-only/capabilities dropped, through the existing bounded package-command/CID capture/cleanup mechanism. Do not use an old image alias or a fabricated namespace mount. The script reads the installed image itself; no source/helper application directory is mounted over it.

Equivalent argv (the existing package runner supplies the actual candidate image and owns exact cleanup):

```
<docker> run --rm --read-only --network none --user 65532:65532 \
  --cap-drop ALL --security-opt no-new-privileges --pids-limit 32 --memory 128m \
  --entrypoint /usr/local/bin/dispatcharr-python <actual-Candidate35-image-ID> \
  -I -B -c '<exact namespace_guest.py bytes>'
```

The real installed0755 empty directory, actual namespace spec/import and single pinned path are tested before manifest assertions. Old packaging with no directory fails this actual filesystem/import prerequisite. The script then uses actual Django6.0.9 `AppConfig.create('apps.api')` and requires the base class, Original name/label, no AppConfig subclass/model module, untouched unconfigured settings and unpopulated global registry. It checks that no Django database module, Source settings or any other Source app was imported. It never calls setup/populate/import_models/ready or reads the lazy default_auto_field property. No server, worker, SQL or Source body is executed.

Local byte-verified installed Django source was reviewed first: `AppConfig.create` imports the namespace and checks optional apps submodule with module_has_submodule/find_spec, then instantiates the base AppConfig. Its constructor only sets name/module/label/path/None model fields; model import and empty ready are separate methods. `registry.apps` is constructed with installed_apps=None and does not populate. `framework-review.json` binds these actual installed bytes to the previously executed Candidate34 package manifest audit, rather than assuming behavior from another Django version.

Root must require the actual guest passed=true, actual command exit0, bounded output EOF/group settlement, expected same image identity and exact container absence. Preserve a failed probe literally. The result qualifies this namespace packaging boundary only; full Django/owned RPC/bootstrap/delivery/SQL behavior still requires the real application batch.

The actual Candidate35 execution subsequently passed in the [Python-host package report](../package/python-host.json), with actual command exit0 and exact cleanup. This remains namespace metadata qualification; it does not establish full Django setup or RPC readiness.
