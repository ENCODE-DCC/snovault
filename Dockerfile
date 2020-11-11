FROM ubuntu:18.04

#CONDA INSTALL PYTHON 3.7
# Anaconda installing
RUN wget https://repo.anaconda.com/archive/Anaconda3-2020.02-Linux-x86_64.sh
RUN bash Anaconda3-2020.02-Linux-x86_64.sh -b
RUN rm Anaconda3-2020.02-Linux-x86_64.sh

# Set python path to conda
ENV PATH /root/anaconda3/bin:$PATH

# Updating Anaconda packages
RUN conda update conda
RUN conda update anaconda
RUN conda update --all

#INSTALL POSTGRES 11 
# add the repository
RUN tee /etc/apt/sources.list.d/pgdg.list <<END
deb http://apt.postgresql.org/pub/repos/apt/ bionic-pgdg main
END
# get the signing key and import it
RUN wget https://www.postgresql.org/media/keys/ACCC4CF8.asc
RUN apt-key add ACCC4CF8.asc
# fetch the metadata from the new repo
RUN apt-get update
RUN apt-get install postgresql-11
RUN apt-get install libpq-dev

#INSTALL NODE 10 
RUN apt -y install curl dirmngr apt-transport-https lsb-release ca-certificates
RUN curl -sL https://deb.nodesource.com/setup_10.x | sudo bash
RUN apt update
RUN apt -y install gcc g++ make nodejs

#UPDATE npm 
RUN npm cache clean --force
RUN npm install -g npm@latest

#INSTALL RUBY
RUN apt install ruby-full
RUN gem install compass


#INSTALL JAVA
RUN apt install openjdk-11-jre-headless

#ELASTICSEARCH 5.6
RUN wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | sudo apt-key add -
RUN apt -y install apt-transport-https
echo "deb https://artifacts.elastic.co/packages/5.x/apt stable main" | sudo tee -a /etc/apt/sources.list.d/elastic-5.x.list
RUN apt-get update

#OTHER DEPENDENCIES
RUN apt-get install -y nginx graphviz redis 

#INSTALL SNOVAULT
WORKDIR $HOME
RUN git clone https://github.com/ENCODE-DCC/snovault.git
WORKDIR $HOME/snovault 
RUN pip install -r requirements.txt
ENV PATH /root/.local/bin:$PATH
RUN make clean && buildout bootstrap && bin/buildout

EXPOSE 6543

CMD ["bin/dev-servers development.ini --app-name app --clear --init --load","bin/pserve development.ini"]

