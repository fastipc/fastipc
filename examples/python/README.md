# Python examples

The Python server and client of the repository's [README](../../README.md#python), using the binding `fipc`
(`pip install fipc-python`).

```bash
python server.py                # in one terminal
python client.py                # in another: b'PING'
```

The pair works with every other language's server or client: an RPC request with opcode 1 and the payload `ping`,
answered with `PING`. In a checkout, put [`bindings/python`](../../bindings/python) on `PYTHONPATH` instead of
installing the package: the binding then loads the checkout's `zig-out` build. The repository's interop test (`python
devtool.py test interop`) runs both against every language's examples.
