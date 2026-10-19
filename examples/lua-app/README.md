## Routes of the demo

| route | what it shows |
|---|---|
| `GET /lua` | the request as the application sees it; `served` counts this worker's requests |
| `GET /lua/hello/{name}` | a path parameter (`claviron.router`) |
| `GET /lua/count` | a reply whose body is a function: sent piece by piece |
| `POST /lua/echo` | the body read with `request:body()`, decoded by `claviron.json` |
| `POST /lua/words` | words and numbers found with `claviron.regex` |

`public/index.html` calls each of them from the browser.

Built with:

```sh
curl -L -R -O https://www.lua.org/ftp/lua-5.5.1.tar.gz
tar zxf lua-5.5.1.tar.gz && cd lua-5.5.1
make all test && make install INSTALL_TOP=$HOME/.local

curl -L -R -O https://luarocks.org/releases/luarocks-3.13.0.tar.gz
tar zxf luarocks-3.13.0.tar.gz && cd luarocks-3.13.0
./configure --prefix=$HOME/.local --with-lua=$HOME/.local --lua-version=5.5
make && make install
```