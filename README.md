
# How to run (Basic)

This repository uses poetry for dependency management.

1. Install poetry from here, depending on OS: https://python-poetry.org/docs/#installing-with-the-official-installer
2. Run `poetry install`
3. Export your `DARWIN_USERNAME` and `DARWIN_PASSWORD`. These can be obtained from the national rail website: https://opendata.nationalrail.co.uk/
4. Run `poetry run python3 src/cli.py`

This will utilise the basic Standard Out writer, printing any collected data to your command line.