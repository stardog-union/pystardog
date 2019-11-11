# pystardog Jupyter Notebook

Follow these steps to run the interactive pystardog tutorial.

## 1. Download and install Stardog

1. Follow the directions [here](https://www.stardog.com/get-started/) to
set up Stardog on your computer.

2. Test that Stardog is running with

    ```shell
    $ stardog-admin server status
    ```

## 2. Create a Python virtualenv and install requirements

This tutorial assumes that you are running Python 3.3 or newer. If
that is not the case, follow [this
page](https://packaging.python.org/guides/installing-using-pip-and-virtual-environments/)
to get a virtualenv created instead of the following steps. I keep my
virtualenvs in `~/.virtualenvs`. If you use a different directory plug
it in instead.

1. Create a virtualenv

    ```bash
    python3 -m venv ~/.virtualenvs/tutorial
    ```
2. Activate the virtualenv

    ```bash
    . ~/.virtualenvs/tutorial/bin/activate
    ```

3. Install requirements

    ```bash
    pip install pystardog jupyterlab pandas seaborn
    ```

## 3. Download the sample dataset

Place these two files in the `notebooks` directory.

[Schema](https://github.com/stardog-union/stardog-tutorials/raw/master/music/music_schema.ttl)

[Data](https://github.com/stardog-union/stardog-tutorials/raw/master/music/music.ttl.gz)

## 4. Open the Jupyter notebook

    ```bash
    cd <path to pystardog repo>/notebooks
    jupyter notebook
    ```
