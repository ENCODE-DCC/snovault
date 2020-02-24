SnoVault JSON-LD Database Framework
===================================

## System Installation (OSX Catlina 10.15.2)
    The instructions below are not guaranteed to work.  See the dependency's website for
    furthur instruction.  We will try to keep this updated as OSX, Xcode, and brew update.  These
    instructions were also tested on Mojave 10.14.6.

1. Command line tools
    ```
    $ xcode-select --install
    ```

1. brew: https://brew.sh/

1. Python3
    ```
    3.6.9 and 3.7.6 have been tested
    3.8 does not work as of Feb 2020
    ```

1. Postgres@11 (Postgres@9.3 also works)
    ```
    $ brew install postgresql@11
    # Only do the following commands if it doesn't exists in ~/.zshrc
    $ echo 'export PATH="/usr/local/opt/postgresql@11/bin:$PATH"' >> ~/.zshrc
    $ source ~/.zshrc
    ```

1. Node 10.x.x
    ```
    $ brew install node@10
    # Only do the following commands if it doesn't exists in ~/.zshrc
    $ echo 'export PATH="/usr/local/opt/postgresql@11/bin:$PATH"' >> ~/.zshrc
    $ source ~/.zshrc
    ```

1. Ruby - Non system version to install compass while avoiding permission errors
    ```
    $ brew install ruby
    # Only do the following commands if it doesn't exists in ~/.zshrc
    $ echo 'export PATH="/usr/local/opt/ruby/bin:$PATH"' >> ~/.zshrc
    $ source ~/.zshrc
    ```

1. Compass
    ```
    $ gem install compass
    # Only do the following commands if the compass location does not exist
    $ ln -s /usr/local/lib/ruby/gems/2.6.0/bin/compass /usr/local/opt/ruby/bin/compass
    ```

1. Java 8 (Java 11 has also been used)
    ```
    $ brew tap AdoptOpenJDK/openjdk
    $ brew cask install adoptopenjdk8
    ```

1. Elasticsearch 5.x
    ```
    $ brew install elasticsearch@5.6
    # Only do the following commands if it doesn't exists in ~/.zshrc
    $ echo 'export PATH="/usr/local/opt/elasticsearch@5.6/bin:$PATH"' >> ~/.zshrc
    $ source ~/.zshrc
    ```

1. Brew dependencies
    ```
    $ brew install libmagic nginx graphviz
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

1. Clone the repo and install requirements
    ```
    $ cd snovault
    $ pip install -r requirements.osx.catalina.txt
    ```

1. Build Application
    ```
    # Make sure you are in the snovault-venv
    $ make clean && buildout bootstrap && bin/buildout
    ```

1. Run Application
    ```
    # Make sure you are in the snovault-venv
    $ bin/dev-servers development.ini --app-name app --clear --init --load
    # In a separate terminal, make sure you are in the snovault-venv
    $ bin/pserve development.ini
    ```

1. Browse to the interface at http://localhost:6543

1. Run Tests
    * no argument runs non bdd tests

    ```
    # Make sure you are in the snovault-venv
    $ ./circle-tests.sh bdd
    $ ./circle-tests.sh npm
    $ ./circle-tests.sh
    ```
