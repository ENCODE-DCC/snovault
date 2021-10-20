SnoVault JSON-LD Database Framework
===================================

System Installation (OSX Big Sur(testing), Catlina(10.15.x), Mojave(10.14.6))
------------------------------------------------------------------------------

| We will try to keep this updated as OSX, Xcode, and brew update.  However the steps below are
  examples and not guaranteed to work for your specific system.  See the dependency's website for
  detailed instructions or let us know of any changes with a pull request.

1. Command line tools
    .. code-block:: bash

        xcode-select --install

2. brew: https://brew.sh/ . Make sure git is installed

3. Python 3.8.5

4. Postgres\@11 (Postgres\@9.3 also works)
    .. code-block:: bash

        brew install postgresql@11
        # May need to add postgres to PATH in your shell profile, e.g. ~/.bash_profile, ~/.zshrc
        # echo 'export PATH="/usr/local/opt/postgresql@11/bin:$PATH"' >> YOUR_SHELL_PROFILE

5. Node 10.x.x
    .. code-block:: bash

        brew install node@12

    You may need to link ``node``/``npm`` with ``brew link node@12`` then add it to your ``PATH``

6. Ruby - Non system version to install compass while avoiding permission errors
    .. code-block:: bash

        brew install ruby
        # May need to add ruby to your bash_profile/zshrc and restart terminal

7. Compass
    .. code-block:: bash

        gem install compass
        # Test the install
        compass -v
        # If the command is not found then find your ruby bin directory
        ls /usr/local/lib/ruby/gem/
        # If you have two versions use the active one
        ruby -v
        # Using the correct ruby version bin diretory, make a sym link
        ln -s /usr/local/lib/ruby/gems/2.6.0/bin/compass /usr/local/opt/ruby/bin/compass

8. Java 11
    .. code-block:: bash

        brew install openjdk@11
        # Add to your PATH in terminal profile, i.e. ~/.bash_profile or ~/.zshrc
        export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)

9. Elasticsearch 5.x
    .. code-block:: bash

        # Download tar: https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-5.6.0.tar.gz

        # Decompress
        tar -xvf ~/Downloads/elasticsearch-5.6.0.tar.gz -C /usr/local/opt/

        # Add to PATH in your terminal profile, i.e. ~/.bash_profile or ~/.zshrc
        export PATH="/usr/local/opt/elasticsearch-5.6.0/bin:$PATH"

        # Restart terminal and check versions
        elasticsearch -V

10. Brew dependencies
        .. code-block:: bash

            brew install libmagic nginx graphviz redis

11. Chrome driver for Testing

        `Chromedriver <https://chromedriver.chromium.org/downloads>`_ is needed in your PATH.
        If working in a python virtual environment, then the chromedriver can be added to
        your-venv-dir/bin directory.

        You also need to install Chrome (if not already installed).
        In addition, allow ``chromedriver`` (System Preferences->Security & Privacy) to run to run bdd tests

Application Installation
========================

1. Create a virtual env in your work directory. Here we use python3 venv module.  Use venv, like conda, if you please
    .. code-block:: bash

        cd your-work-dir
        python3 -m venv snovault-venv
        source snovault-venv/bin/activate
        pip install -U pip==21.0.1

2. Clone the repo and install requirements
    .. code-block:: bash

        cd snovault
        pip install -e '.[dev]'

    If psycopg2 fails to compile, you may need to set LDFLAGS to the output of ``pg_config --ldflags`` before pip installation.
        .. code-block:: bash

            LDFLAGS=$(pg_config --ldflags) pip install -e '.[dev]'

    If you have errors at runtime that look like this::

        ImportError: dlopen(/Users/foo/venv/lib/python3.7/site-packages/psycopg2/_psycopg.cpython-37m-darwin.so, 2): Symbol not found: _PQencryptPasswordConn
        Referenced from: /Users/foo/venv/lib/python3.7/site-packages/psycopg2/_psycopg.cpython-37m-darwin.so
        Expected in: /usr/lib/libpq.5.dylib
        in /Users/foo/venv/lib/python3.7/site-packages/psycopg2/_psycopg.cpython-37m-darwin.so

    you may need to add the ``brew``-installed Postgres headers, usually ``-L/usr/local/opt/postgresql@11/lib``, to the ``LDFLAGS`` in addition to the ones given by ``pg_config --ldflags``.

3. Build Application
    .. code-block:: bash

        # Make sure you are in the snovault-venv
        make clean && buildout

4. Run Application
    .. code-block:: bash

        # Make sure you are in the snovault-venv
        dev-servers development.ini --app-name app --clear --init --load
        # In a separate terminal, make sure you are in the snovault-venv
        pserve development.ini

5. Browse to the interface at http://localhost:6543

6. Run Tests
    * no argument runs non bdd tests

    .. code-block:: bash

        # Make sure you are in the snovault-venv
        ./circle-tests.sh bdd
        ./circle-tests.sh npm
        ./circle-tests.sh

