SnoVault JSON-LD Database Framework
===================================

## System Installation (OSX Catlina 10.15.2)
    We will try to keep this updated as OSX, Xcode, and brew update, but cannot guarantee the 
    'System Installation' commands will work on your system.  

1. Command line tools
    ```
    $ xcode-select --install
    ```

1. brew: https://brew.sh/

1. Python 3.7.x: Native on catalina

1. Postgres@11
    ```
    $ brew install postgresql@11
    # Only do the following commands if it doesn't exists in ~/.zshrc
    $ echo 'export PATH="/usr/local/opt/postgresql@11/bin:$PATH"' >> ~/.zshrc
    $ source ~/.zshrc
    ```

1. Node 10.x
    ```
    $ brew install node@10
    # Only do the following commands if it doesn't exists in ~/.zshrc
    $ echo 'export PATH="/usr/local/opt/postgresql@11/bin:$PATH"' >> ~/.zshrc
    $ source ~/.zshrc
    ```

1. Ruby - Non system version to install compass
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

1. Java 8
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


## Application Installation

1. Create a virtual env in your work directory
    ```
    $ python3 -m venv .venv
    $ source .venv/bin/activate
    ```

1. Checkout repo and install requirements
    ```
    $ git clone https://github.com/ENCODE-DCC/snovault.git
    $ cd snovault
    $ pip install -r requirements.osx.catalina.txt

    # Psycopg2/openssl error on pip insall
    As of 2020-01-27 it was required to export openssl flags in order to pip install psycopg2.
    If there is a psycopg2 issue on pip install of requirements then export the flags below
    and try to install the requirements again.
        
    $ export LDFLAGS="-I/usr/local/opt/openssl/include -L/usr/local/opt/openssl/lib"
    ```

1. Build Application
    ```
    $ make clean && buildout bootstrap && bin/buildout
    ```

1. Run Application
    ```
    $ bin/dev-servers development.ini --app-name app --clear --init --load
    # In a separate terminal
    $ bin/pserve development.ini
    ```

1. Browse to the interface at http://localhost:6543

1. Run Tests
    * no argument runs non bdd tests
    * [Chromedriver](https://chromedriver.chromium.org/downloads) is needed in your PATH for bdd tests.
    ```
    $ ./circle-tests.sh bdd
    $ ./circle-tests.sh npm
    $ ./circle-tests.sh
    ```
