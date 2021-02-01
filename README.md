SnoVault JSON-LD Database Framework
===================================

## System Installation (OSX Big Sur(testing), Catlina(10.15.x), Mojave(10.14.6))
    We will try to keep this updated as OSX, Xcode, and brew update.  However the steps below are
    examples and not guaranteed to work for your specific system.  See the dependency's website for
    detailed instructions or let us know of any changes with a pull request.

1. Command line tools
    ```
    $ xcode-select --install
    ```

1. brew: https://brew.sh/
    ```
    Make sure git is installed
    ```

1. Python3
    ```
    3.6.9 and 3.7.6 have been tested
    3.8 does not work as of Feb 2020
    ```

1. Postgres@11 (Postgres@9.3 also works)
    ```
    $ brew install postgresql@11
    ```

1. Node 10.x.x
    ```
    $ brew install node@10
    ```

1. Ruby - Non system version to install compass while avoiding permission errors
    ```
    $ brew install ruby
    # May need to add ruby to your bash_profile/zshrc and restart terminal
    ```

1. Compass
    ```
    $ gem install compass
    # Test the install
    $ compass -v
    # If the command is not found then find your ruby bin directory
    $ ls /usr/local/lib/ruby/gem/
    # If you have two versions use the active one
    ruby -v
    # Using the correct ruby version bin diretory, make a sym link
    $ ln -s /usr/local/lib/ruby/gems/2.6.0/bin/compass /usr/local/opt/ruby/bin/compass
    ```

1. Java 8 (Java 11 has also been used)
    ```
    $ brew install adoptopenjdk8
    # Add to you PATH in terminal profile, i.e. ~/.bash_profile or ~/.zshrc
    export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)
    ```

1. Elasticsearch 5.x
    ```
    Download tar: https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-5.6.0.tar.gz

    # Decompress
    $ tar -xvf ~/Downloads/elasticsearch-5.6.0.tar.gz -C /usr/local/opt/

    # Add to PATH in your terminal profile, i.e. ~/.bash_profile or ~/.zshrc
    $ export PATH="/usr/local/opt/elasticsearch-5.6.0/bin:$PATH"

    # Restart terminal and check versions
    $ elasticsearch -V
    ```

1. Brew dependencies
    ```
    $ brew install libmagic nginx graphviz redis
    ```

1. Chrome driver for Testing
    ```
    [Chromedriver](https://chromedriver.chromium.org/downloads) is needed in your PATH.
    If working in a python virtual environment, then the chromedriver can be added to
    your-venv-dir/bin directory.
    ```


## Application Installation

1. Create a virtual env in your work directory.  
    Here we use python3 venv module.  Use venv, like conda, if you please
    ```
    $ cd your-work-dir
    $ python3 -m venv snovault-venv
    $ source snovault-venv/bin/activate
    ```

2. Clone the repo and install requirements
    ```
    $ cd snovault
    $ pip install -e '.[dev]'
    ```

3. Build Application
    ```
    # Make sure you are in the snovault-venv
    $ make clean && buildout
    ```

4. Run Application
    ```
    # Make sure you are in the snovault-venv
    $ dev-servers development.ini --app-name app --clear --init --load
    # In a separate terminal, make sure you are in the snovault-venv
    $ pserve development.ini
    ```

5. Browse to the interface at http://localhost:6543

6. Run Tests
    * no argument runs non bdd tests

    ```
    # Make sure you are in the snovault-venv
    $ ./circle-tests.sh bdd
    $ ./circle-tests.sh npm
    $ ./circle-tests.sh
    ```
