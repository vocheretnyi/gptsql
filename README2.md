## Install poetry:
```shell
curl -sSL https://install.python-poetry.org | python3 -
```
It will add the `poetry` command to Poetry's bin directory, located at:

`/Users/vocheretnyi/.local/bin`

To get started you need Poetry's bin directory (`/Users/vocheretnyi/.local/bin`) in your `PATH`
environment variable.

Add `export PATH="/Users/vocheretnyi/.local/bin:$PATH"` to your shell configuration file.
In my case it's `~/.zshrc` file.

Then run `source ~/.zshrc` to apply the changes.

## Some dependencies:
```shell
brew install mysql pkg-config
pip install mysqlclient
pip install singlestoredb
```

Not sure (?):
```shell
pip install SQLAlchemy
pip install PyMySQL
```
## Run:
`poetry shell`

`python -m gptsql`

## Install postgresql (optional):
`brew install postgresql`
For more details, read:
  https://www.postgresql.org/docs/14/app-initdb.html
```shell
Or, if you don't want/need a background service you can just run:
  /usr/local/opt/postgresql@14/bin/postgres -D /usr/local/var/postgresql@14
pg_config --version
```

```shell
brew services start postgresql@14
psql postgres
initdb /usr/local/var/postgres
pg_ctl -D /usr/local/var/postgres start
psql -U vocheretnyi -d postgres
```